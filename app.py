# ---------------------------------------------------------------------------
# worker.py (single file)
# Purpose:
#   - Long-poll AWS SQS and process messages safely (no loops)
#   - CloudWatch JSON logger (singleton) with per-user/deployment context
#   - Group messages in batches by (agent_id, repository) and analyze together
#   - Build compact digest + call Anthropic Claude → strict JSON report
#   - Log LLM report and save report artifact locally
#
# Env:
#   SQS_QUEUE_URL=...
#   AWS_DEFAULT_REGION=us-east-1
#   AWS_ACCESS_KEY_ID=...  AWS_SECRET_ACCESS_KEY=... [AWS_SESSION_TOKEN=?]
#   # LLM:
#   ANTHROPIC_AUTH_TOKEN=sk-...
#   ANTHROPIC_MODEL=claude-3-haiku-20240307
#   LLM_MAX_OUTPUT_TOKENS=600
#   LLM_TEMPERATURE=0
#   # Logging:
#   LOG_LEVEL=info
#   ENABLE_CW_LOGS=true|false
#   CLOUDWATCH_GROUP=refinement-agents
#   CLOUDWATCH_STREAM_PREFIX=dev
#   # Safeguards:
#   DEDUPLICATION_MINUTES=10
# ---------------------------------------------------------------------------

from __future__ import annotations

import json
import logging
import os
import re
import signal
import threading
import time
import hashlib
from contextvars import ContextVar
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Literal, Dict, Tuple

import boto3
from botocore.exceptions import ClientError
from pydantic import BaseModel, Field, ValidationError

# ======================= CloudWatch JSON logger (singleton) ===================

_ctx_user: ContextVar[str] = ContextVar("ctx_user", default="-")
_ctx_deploy: ContextVar[str] = ContextVar("ctx_deploy", default="-")


class JsonFormatter(logging.Formatter):
    """Emit logs as JSON so they are easy to search in CloudWatch."""
    def format(self, record: logging.LogRecord) -> str:
        base = {
            "ts": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "msg": record.getMessage(),
            "user": _ctx_user.get(),
            "deployment": _ctx_deploy.get(),
            "logger": record.name,
        }
        if record.exc_info:
            base["exc_info"] = self.formatException(record.exc_info)
        if hasattr(record, "kv"):  # logger.info("...", extra={"kv": {...}})
            base.update(record.kv)
        return json.dumps(base, ensure_ascii=False)


class _CWLoggerSingleton:
    _lock = threading.Lock()
    _logger: logging.Logger | None = None

    @classmethod
    def get(cls) -> logging.Logger:
        with cls._lock:
            if cls._logger:
                return cls._logger

            logger = logging.getLogger("refinement")
            logger.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper()))
            logger.handlers.clear()

            ch = logging.StreamHandler()
            ch.setFormatter(JsonFormatter())
            logger.addHandler(ch)

            if os.getenv("ENABLE_CW_LOGS", "false").lower() == "true":
                if not watchtower:
                    raise RuntimeError("ENABLE_CW_LOGS=true, but 'watchtower' not installed")
                group = os.getenv("CLOUDWATCH_GROUP", "refinement-agents")
                stream_prefix = os.getenv("CLOUDWATCH_STREAM_PREFIX", "dev")
                cwh = watchtower.CloudWatchLogHandler(
                    log_group=group,
                    stream_name=f"{stream_prefix}-%(asctime)s",
                    create_log_group=True,
                    send_interval=5,
                    max_batch_count=50,
                )
                cwh.setFormatter(JsonFormatter())
                logger.addHandler(cwh)

            cls._logger = logger
            return logger


def get_logger() -> logging.Logger:
    return _CWLoggerSingleton.get()


def set_log_context(user: str | None = None, deployment: str | None = None):
    """Set per-request context (user/deployment) to appear in every log line."""
    if user is not None:
        _ctx_user.set(user)
    if deployment is not None:
        _ctx_deploy.set(deployment)


logger = get_logger()

# ============================== Shutdown control ==============================

SHUTDOWN = False


def _handle_shutdown(*_):
    """SIGINT/SIGTERM handler to exit"""
    global SHUTDOWN
    SHUTDOWN = True
    logger.info("Shutdown requested")


signal.signal(signal.SIGINT, _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)

# ================================ Dedup store =================================

_SEEN: Dict[str, datetime] = {}
_SEEN_TTL_MIN = int(os.getenv("DEDUPLICATION_MINUTES", "10"))
_SEEN_LOCK = threading.Lock()


def _dedup_key(message: dict) -> str:
    """Stable deduplication key from MessageId+Body."""
    mid = message.get("MessageId", "")
    body = message.get("Body", "")
    return hashlib.sha256((mid + "|" + body).encode()).hexdigest()


def seen_recently(key: str) -> bool:
    with _SEEN_LOCK:
        exp = _SEEN.get(key)
        if not exp:
            return False
        if datetime.utcnow() > exp:
            del _SEEN[key]
            return False
        return True


def mark_seen(key: str) -> None:
    with _SEEN_LOCK:
        _SEEN[key] = datetime.utcnow() + timedelta(minutes=_SEEN_TTL_MIN)

# ================================ LLM part ====================================

ANTHROPIC_KEY = os.getenv("ANTHROPIC_AUTH_TOKEN")
MODEL = os.getenv("ANTHROPIC_MODEL", "claude-3-haiku-20240307")
MAX_OUTPUT_TOKENS = int(os.getenv("LLM_MAX_OUTPUT_TOKENS", "600"))
TEMP = float(os.getenv("LLM_TEMPERATURE", "0"))


class Report(BaseModel):
    """Strict schema for deterministic JSON report."""
    severity: Literal["info", "warn", "error", "critical"]
    summary: str = Field(..., description="2–3 sentence overview")
    top_findings: List[str]
    probable_causes: List[str]
    next_steps: List[str]
    impacted_components: List[str]
    evidence: List[str] = Field(default_factory=list)
    timeline: List[str] = Field(default_factory=list)


_ERROR_PAT = re.compile(r"(?i)\b(error|exception|failed|traceback|crash|panic|fatal)\b")
_WARN_PAT = re.compile(r"(?i)\b(warn|warning|timeout|throttle)\b")


def build_log_digest(raw: str, max_chars: int = 8000) -> str:
    """Compact digest: error/warn lines + recent tail; dedup; hard cap length."""
    if not raw:
        return ""
    lines = raw.splitlines()
    errors = [l for l in lines if _ERROR_PAT.search(l)]
    warns = [l for l in lines if _WARN_PAT.search(l)]
    tail = "\n".join(lines[-500:])
    merged: list[str] = []
    seen: set[str] = set()
    for group in (errors[-100:], warns[-100:], [tail]):
        for l in (group if isinstance(group, list) else [group]):
            h = hashlib.sha256(l.encode()).hexdigest()
            if h in seen:
                continue
            seen.add(h)
            merged.append(l)
    digest = "\n".join(merged)
    if len(digest) > max_chars:
        digest = digest[-max_chars:]
    return digest


def _anthropic_client() -> Anthropic:
    if not ANTHROPIC_KEY or Anthropic is None:
        raise RuntimeError("ANTHROPIC_AUTH_TOKEN missing or anthropic SDK not installed")
    return Anthropic(api_key=ANTHROPIC_KEY)


_SYS_PROMPT = (
    "You are a senior incident analyst for CI/CD and cloud deployments. "
    "Read logs and produce a crisp, actionable incident report."
)

_USER_TMPL = """Analyze the following deployment/runtime logs and return ONLY valid JSON following the schema.

Context:
- repo: {repo}
- pr_id: {pr_id}
- agent_id: {agent_id}

Logs (digest):
{digest}

JSON SCHEMA (output exactly this shape, no extra keys, no markdown):
{schema}
"""


def analyze_logs_with_llm(digest: str, repo: str, pr_id: int, agent_id: str,
                          retries: int = 3, backoff: float = 1.5) -> Report:
    """Call Anthropic Claude and validate JSON output (with retries/backoff)."""
    payload = _USER_TMPL.format(
        repo=repo, pr_id=pr_id, agent_id=agent_id,
        digest=digest,
        schema=json.dumps(Report.model_json_schema(), ensure_ascii=False, indent=2),
    )
    client = _anthropic_client()
    last_err: Optional[Exception] = None

    for attempt in range(1, retries + 1):
        try:
            resp = client.messages.create(
                model=MODEL,
                temperature=TEMP,
                max_tokens=MAX_OUTPUT_TOKENS,
                messages=[
                    {"role": "system", "content": _SYS_PROMPT},
                    {"role": "user", "content": payload},
                ],
            )
            parts = []
            for c in resp.content:
                if getattr(c, "type", None) == "text":
                    parts.append(getattr(c, "text", ""))
            text = "".join(parts).strip()
            data = json.loads(text)
            return Report.model_validate(data)
        except (RateLimitError, APIError) as e:  # type: ignore
            last_err = e
            sleep_s = backoff ** attempt
            logger.warning(f"LLM transient error (attempt {attempt}): {e}; retry in {sleep_s:.1f}s")
            time.sleep(sleep_s)
        except (json.JSONDecodeError, ValidationError) as e:
            last_err = e
            payload += "\n\nREMINDER: Output must be STRICT JSON only. No markdown."
            sleep_s = backoff ** attempt
            logger.warning(f"LLM JSON/validation error (attempt {attempt}): {e}; retry in {sleep_s:.1f}s")
            time.sleep(sleep_s)

    raise RuntimeError(f"LLM analysis failed after {retries} attempts: {last_err}")

# =============================== SQS Worker ===================================

class SQSWorker:
    """SQS worker: long-poll + visibility heartbeat + dedup + batch LLM analysis."""

    def __init__(self) -> None:
        self.queue_url = os.environ["SQS_QUEUE_URL"]
        region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        self.sqs = boto3.client("sqs", region_name=region)

    # ----------- polling ------------

    def poll_messages(self) -> List[dict]:
        """Long-poll SQS for up to 5 messages."""
        try:
            resp = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=5,
                WaitTimeSeconds=20,
                MessageAttributeNames=["All"],
            )
            return resp.get("Messages", [])
        except ClientError as e:
            logger.error(f"SQS receive error: {e}")
            time.sleep(5)
            return []

    # ----------- grouping for batch analysis ------------

    def group_by_actor(self, messages: List[dict]) -> Dict[Tuple[str, str], List[dict]]:
        """Group messages by (agent_id, repository_name)."""
        groups: Dict[Tuple[str, str], List[dict]] = {}
        for m in messages:
            try:
                body = json.loads(m["Body"])
                agent = str(body.get("agent_id", "-"))
                repo = str(body.get("repository_name", "-"))
                groups.setdefault((agent, repo), []).append(m)
            except Exception:
                groups.setdefault(("-", "-"), []).append(m)
        return groups

    # ----------- visibility heartbeat ------------

    def _start_heartbeat(self, receipt_handle: str) -> tuple[list[bool], threading.Thread]:
        """Start a background thread that keeps the message invisible."""
        _done = [False]

        def _extend():
            while not _done[0] and not SHUTDOWN:
                time.sleep(10)
                try:
                    self.sqs.change_message_visibility(
                        QueueUrl=self.queue_url,
                        ReceiptHandle=receipt_handle,
                        VisibilityTimeout=60,
                    )
                except Exception as ex:
                    logger.warning(f"Visibility extend failed: {ex}")

        t = threading.Thread(target=_extend, daemon=True)
        t.start()
        return _done, t

    # ----------- single message processing (used as fallback) ------------

    def process_message(self, message: dict) -> Optional[Report]:
        """Process a single message (non-batch path)."""
        dedup = _dedup_key(message)
        if seen_recently(dedup):
            logger.info("↩️ Duplicate within TTL – skipping without analysis")
            return None

        receipt = message["ReceiptHandle"]
        done, _t = self._start_heartbeat(receipt)

        try:
            try:
                body = json.loads(message.get("Body", ""))
            except json.JSONDecodeError:
                body = message.get("Body", "")

            repo = (body.get("repository_name") if isinstance(body, dict) else "unknown-repo") or "unknown-repo"
            pr_id = int(body.get("pr_id") if isinstance(body, dict) and body.get("pr_id") is not None else -1)
            agent = str(body.get("agent_id") if isinstance(body, dict) else "unknown-agent")

            set_log_context(user=agent, deployment=f"{repo}#PR{pr_id}")

            raw_log = body if isinstance(body, str) else json.dumps(body, ensure_ascii=False)
            digest = build_log_digest(raw_log, max_chars=8000)

            try:
                report = analyze_logs_with_llm(digest=digest, repo=repo, pr_id=pr_id, agent_id=agent)
                logger.info("🧠 LLM report assembled",
                            extra={"kv": {"severity": report.severity, "repo": repo, "pr": pr_id,
                                          "next_steps": report.next_steps[:3]}})
            except Exception as e:
                logger.warning(f"LLM analysis failed, using fallback: {e}")
                sev = "error" if "error" in (raw_log or "").lower() else "warn"
                report = Report(
                    severity=sev,
                    summary="Heuristic summary due to LLM failure.",
                    top_findings=["See digest tail for last events"],
                    probable_causes=["LLM API error or insufficient context"],
                    next_steps=["Retry analysis later", "Check CI/CD logs for root error"],
                    impacted_components=[],
                    evidence=(digest.splitlines()[-5:] if digest else []),
                    timeline=[],
                )

            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            artifacts = Path("./artifacts"); artifacts.mkdir(parents=True, exist_ok=True)
            out = artifacts / f"{ts}_pr{pr_id}_report.json"
            out.write_text(report.model_dump_json(indent=2, ensure_ascii=False))
            logger.info(f"📄 Report saved", extra={"kv": {"path": str(out)}})

            # mark dedup so repeated deliveries won't burn tokens
            mark_seen(dedup)
            return report

        finally:
            # stop heartbeat in any case
            done[0] = True

    # ----------- batch processing ------------

    def process_batch(self, agent: str, repo: str, batch: List[dict]) -> Optional[Report]:
        """
        Analyze multiple messages at once (one LLM call) for the same (agent, repo).
        We still start heartbeats for each message to avoid re-pick by other workers.
        """
        # Build joined logs, start all heartbeats, handle dedup
        joined_logs: List[str] = []
        heartbeats: List[list[bool]] = []
        dedup_keys: List[str] = []
        receipts: List[str] = []

        for m in batch:
            dk = _dedup_key(m)
            if seen_recently(dk):
                continue
            dedup_keys.append(dk)
            receipts.append(m["ReceiptHandle"])
            done, _t = self._start_heartbeat(m["ReceiptHandle"])
            heartbeats.append(done)

            try:
                body = json.loads(m.get("Body", ""))
            except json.JSONDecodeError:
                body = m.get("Body", "")
            raw = body if isinstance(body, str) else json.dumps(body, ensure_ascii=False)
            joined_logs.append(raw)

        if not joined_logs:
            return None

        set_log_context(user=agent, deployment=f"{repo}#batch")
        digest = build_log_digest("\n---\n".join(joined_logs), max_chars=8000)

        # Using pseudo PR id -1 for batch reports (no single PR context)
        try:
            report = analyze_logs_with_llm(digest=digest, repo=repo, pr_id=-1, agent_id=agent)
            logger.info("Batch LLM report assembled",
                        extra={"kv": {"repo": repo, "user": agent, "batch_size": len(joined_logs),
                                      "severity": report.severity}})
        except Exception as e:
            logger.warning(f"LLM batch analysis failed, using fallback: {e}")
            sev = "error" if "error" in digest.lower() else "warn"
            report = Report(
                severity=sev,
                summary="Heuristic batch summary due to LLM failure.",
                top_findings=["See digest tail"],
                probable_causes=["LLM API error or insufficient context"],
                next_steps=["Retry analysis later", "Check CI/CD logs"],
                impacted_components=[],
                evidence=digest.splitlines()[-5:],
                timeline=[],
            )

        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        artifacts = Path("./artifacts"); artifacts.mkdir(parents=True, exist_ok=True)
        out = artifacts / f"{ts}_batch_{agent}_{repo.replace('/','_')}.json"
        out.write_text(report.model_dump_json(indent=2, ensure_ascii=False))
        logger.info("📄 Batch report saved", extra={"kv": {"path": str(out)}})

        # mark all messages as seen (so repeated deliveries won't trigger analysis again)
        for dk in dedup_keys:
            mark_seen(dk)
        # stop all heartbeats
        for d in heartbeats:
            d[0] = True

        return report

    # ----------- deletion -------------

    def delete_message(self, message: dict) -> None:
        try:
            self.sqs.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=message["ReceiptHandle"],
            )
            logger.info("Message deleted from SQS")
        except ClientError as e:
            logger.error(f"Failed to delete message: {e}")

    # ----------- main loop ------------

    def run(self) -> None:
        logger.info("SQS worker started")
        while not SHUTDOWN:
            try:
                msgs = self.poll_messages()
                if not msgs:
                    time.sleep(1)
                    continue

                # group and do batch analysis
                groups = self.group_by_actor(msgs)
                for (agent, repo), batch in groups.items():
                    # 1) batch analysis (one LLM call for the group)
                    self.process_batch(agent, repo, batch)
                    # if we don't need batch
                    # for m in batch: self.process_message(m)

            except KeyboardInterrupt:
                logger.info("Interrupted by user")
                break
            except Exception as e:
                logger.error(f"Unexpected loop error: {e}")
                time.sleep(5)
        logger.info("Worker stopped")


# ---------------------------------- Main -------------------------------------

def main() -> None:
    # Validate required env vars early
    missing = [k for k in ["SQS_QUEUE_URL", "AWS_DEFAULT_REGION"] if not os.getenv(k)]
    if missing:
        raise SystemExit(f"Missing required env vars: {', '.join(missing)}")

    worker = SQSWorker()
    worker.run()


if __name__ == "__main__":
    main()
