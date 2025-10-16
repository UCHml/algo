# -----------------------------------------------------------------------------
# Requirements:
#   pip install boto3 anthropic pydantic
#
# Environment variables:
#   SQS_QUEUE_URL=...
#   AWS_DEFAULT_REGION=us-east-1
#   AWS_ACCESS_KEY_ID=...
#   AWS_SECRET_ACCESS_KEY=...
#   AWS_SESSION_TOKEN=...             # optional
#   ANTHROPIC_AUTH_TOKEN=sk-...       # optional; if missing -> fallback
#   ANTHROPIC_MODEL=claude-3-haiku-20240307
#   LLM_MAX_OUTPUT_TOKENS=600
#   LLM_TEMPERATURE=0
#   DEDUPLICATION_MINUTES=10
# -----------------------------------------------------------------------------

from __future__ import annotations

import json
import logging
import os
import re
import signal
import threading
import time
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Literal

import boto3
from botocore.exceptions import ClientError
from pydantic import BaseModel, Field, ValidationError

# --- Optional Anthropic import (safe if key missing) ---
try:
    from anthropic import Anthropic, APIError, RateLimitError  # type: ignore
except Exception:  # pragma: no cover
    Anthropic = None  # type: ignore
    APIError = Exception  # type: ignore
    RateLimitError = Exception  # type: ignore


# ----------------------------- Logging setup ---------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("sqs-worker")


# ----------------------------- Global control --------------------------------
# Global shutdown flag to stop loops gracefully
SHUTDOWN = False

def _handle_shutdown(*_):
    """SIGINT/SIGTERM handler to exit gracefully."""
    global SHUTDOWN
    SHUTDOWN = True
    logger.info("🔻 Shutdown requested")

signal.signal(signal.SIGINT, _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)


# ----------------------------- Dedup store -----------------------------------
# In-memory idempotency store to avoid re-processing same message within TTL.
_SEEN: dict[str, datetime] = {}
_SEEN_TTL_MIN = int(os.getenv("DEDUPLICATION_MINUTES", "10"))
_SEEN_LOCK = threading.Lock()

def _dedup_key(message: dict) -> str:
    """Create a stable deduplication key from MessageId+Body"""
    mid = message.get("MessageId", "")
    body = message.get("Body", "")
    return hashlib.sha256((mid + "|" + body).encode()).hexdigest()

def seen_recently(key: str) -> bool:
    """Return True if message was processed recently (within TTL)."""
    with _SEEN_LOCK:
        exp = _SEEN.get(key)
        if not exp:
            return False
        if datetime.utcnow() > exp:
            del _SEEN[key]
            return False
        return True

def mark_seen(key: str) -> None:
    """Mark message as processed for TTL minutes."""
    with _SEEN_LOCK:
        _SEEN[key] = datetime.utcnow() + timedelta(minutes=_SEEN_TTL_MIN)


# ----------------------------- LLM analyzer ----------------------------------
ANTHROPIC_KEY = os.getenv("ANTHROPIC_AUTH_TOKEN")
MODEL = os.getenv("ANTHROPIC_MODEL", "claude-3-haiku-20240307")
MAX_OUTPUT_TOKENS = int(os.getenv("LLM_MAX_OUTPUT_TOKENS", "600"))
TEMP = float(os.getenv("LLM_TEMPERATURE", "0"))

class Report(BaseModel):
    """Strict schema for deterministic JSON report."""
    severity: Literal["info","warn","error","critical"]
    summary: str = Field(..., description="Short 2-3 sentence overview")
    top_findings: List[str]
    probable_causes: List[str]
    next_steps: List[str]
    impacted_components: List[str]
    evidence: List[str] = Field(default_factory=list)
    timeline: List[str] = Field(default_factory=list)

_ERROR_PAT = re.compile(r"(?i)\b(error|exception|failed|traceback|crash|panic|fatal)\b")
_WARN_PAT  = re.compile(r"(?i)\b(warn|warning|timeout|throttle)\b")

def build_log_digest(raw: str, max_chars: int = 8000) -> str:
    """Build a compact digest: error/warn lines + recent tail; dedup; cap length."""
    if not raw:
        return ""
    lines = raw.splitlines()
    errors = [l for l in lines if _ERROR_PAT.search(l)]
    warns  = [l for l in lines if _WARN_PAT.search(l)]
    tail = "\n".join(lines[-500:])  # recent context
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
    """Create Anthropic client or raise if key is missing."""
    if not ANTHROPIC_KEY or Anthropic is None:
        raise RuntimeError("ANTHROPIC_AUTH_TOKEN missing or anthropic SDK not installed")
    return Anthropic(api_key=ANTHROPIC_KEY)

_SYS_PROMPT = """You are a senior incident analyst for CI/CD and cloud deployments.
You read raw logs and produce a crisp, actionable incident report.
Be concise, technical, and specific. If uncertain, say so explicitly."""

_USER_TMPL = """Analyze the following deployment/runtime logs and return ONLY valid JSON following the schema.

Context (optional):
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
                    {"role": "user",   "content": payload},
                ]
            )
            # Robust text extraction (Anthropic returns a list of "content" blocks)
            text_parts = []
            for c in resp.content:
                # Each block may have .type == "text" and .text attribute
                if getattr(c, "type", None) == "text":
                    text_parts.append(getattr(c, "text", ""))
            text = "".join(text_parts).strip()
            data = json.loads(text)
            report = Report.model_validate(data)
            return report
        except (RateLimitError, APIError) as e:  # type: ignore
            last_err = e
            sleep_s = backoff ** attempt
            logger.warning(f"LLM transient error (attempt {attempt}): {e}; retrying in {sleep_s:.1f}s")
            time.sleep(sleep_s)
        except (json.JSONDecodeError, ValidationError) as e:
            # Ask model to fix to strict JSON on next attempt
            last_err = e
            payload = payload + "\n\nREMINDER: Output must be STRICT JSON only. No markdown, no explanations."
            sleep_s = backoff ** attempt
            logger.warning(f"LLM JSON/validation error (attempt {attempt}): {e}; retrying in {sleep_s:.1f}s")
            time.sleep(sleep_s)

    raise RuntimeError(f"LLM analysis failed after {retries} attempts: {last_err}")


# ----------------------------- SQS Worker ------------------------------------
class SQSWorker:
    """SQS long-poll worker with visibility heartbeat, dedup and LLM analysis."""

    def __init__(self) -> None:
        self.queue_url = os.environ["SQS_QUEUE_URL"]
        region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        # Explicit region to avoid incorrect defaults
        self.sqs = boto3.client("sqs", region_name=region)

        # (Optional) your repository adapter for PR comments, replace with real one
        self.repository = DummyRepositoryAdapter()

    # -------------------------------------------------------------------------
    def poll_messages(self) -> list[dict]:
        """Long-poll SQS for up to 5 messages (saves CPU and API calls)."""
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

    # -------------------------------------------------------------------------
    def process_message(self, message: dict) -> None:
        """Process a single message: dedup -> heartbeat -> analyze -> save report."""
        # 1) Deduplicate
        dedup = _dedup_key(message)
        if seen_recently(dedup):
            logger.info("↩️ Duplicate within TTL – skipping without analysis")
            return

        # 2) Start visibility heartbeat
        receipt = message["ReceiptHandle"]
        _done = [False]

        def _extend_visibility():
            """Keep SQS message invisible while we are processing."""
            while not _done[0] and not SHUTDOWN:
                time.sleep(10)
                try:
                    self.sqs.change_message_visibility(
                        QueueUrl=self.queue_url,
                        ReceiptHandle=receipt,
                        VisibilityTimeout=60,
                    )
                except Exception as ex:
                    logger.warning(f"Visibility extend failed: {ex}")

        heartbeat = threading.Thread(target=_extend_visibility, daemon=True)
        heartbeat.start()

        try:
            # 3) Parse body (expect JSON or plain text)
            try:
                body_raw = message.get("Body", "")
                body = json.loads(body_raw)
            except json.JSONDecodeError:
                body = message.get("Body", "")

            # Expect these fields in JSON messages (adapt to your schema if needed)
            # If absent, fall back to 'unknown' values so demo still works.
            repository_name = (body.get("repository_name") if isinstance(body, dict) else "unknown-repo") or "unknown-repo"
            pr_id = int(body.get("pr_id") if isinstance(body, dict) and body.get("pr_id") is not None else -1)
            agent_id = str(body.get("agent_id") if isinstance(body, dict) else "unknown-agent")

            # 4) Build compact digest to save LLM tokens
            raw_log = body if isinstance(body, str) else json.dumps(body, ensure_ascii=False)
            digest = build_log_digest(raw_log, max_chars=8000)

            # 5) LLM analysis → strict JSON report (fallback if LLM disabled)
            try:
                report: Report = analyze_logs_with_llm(
                    digest=digest,
                    repo=repository_name,
                    pr_id=pr_id,
                    agent_id=agent_id,
                )
                logger.info("🧠 LLM report assembled")
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
                    evidence=digest.splitlines()[-5:],
                    timeline=[],
                )

            # 6) Persist JSON report locally
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            artifacts = Path("./artifacts")
            artifacts.mkdir(parents=True, exist_ok=True)
            out_path = artifacts / f"{ts}_pr{pr_id}_report.json"
            out_path.write_text(report.model_dump_json(indent=2, ensure_ascii=False))
            logger.info(f"📄 Report saved: {out_path}")

            # 7) Optional: post a short summary to PR (replace with your adapter)
            if pr_id != -1:
                short = (
                    f"**Deployment Incident Report (auto)**\n"
                    f"- Severity: `{report.severity}`\n"
                    f"- Summary: {report.summary}\n"
                    f"- Next steps: " + "; ".join(report.next_steps[:3])
                )
                try:
                    self.repository.comment_on_pr(pr_id, short)
                except Exception as ex:
                    logger.warning(f"PR comment failed: {ex}")

            # 8) Mark dedup to avoid re-analyzing on re-delivery
            mark_seen(dedup)

            # If your team decides to remove SQS messages upon success,
            # uncomment the next line (canonical SQS behavior):
            # self.delete_message(message)

        except Exception as e:
            logger.exception(f"💥 Unexpected error while processing message: {e}")
        finally:
            # Stop visibility heartbeat in any case
            _done[0] = True

    # -------------------------------------------------------------------------
    def delete_message(self, message: dict) -> None:
        """Delete the message from SQS (optional; canonical success behavior)."""
        try:
            self.sqs.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=message["ReceiptHandle"],
            )
            logger.info("✅ Message deleted from SQS")
        except ClientError as e:
            logger.error(f"Failed to delete message: {e}")

    # -------------------------------------------------------------------------
    def run(self) -> None:
        """Main loop: long-poll SQS, process batch, exit gracefully on signal."""
        logger.info("🚀 SQS worker started")
        while not SHUTDOWN:
            try:
                msgs = self.poll_messages()
                if not msgs:
                    # small idle sleep to avoid tight-loop when empty
                    time.sleep(1)
                    continue
                for m in msgs:
                    if SHUTDOWN:
                        break
                    self.process_message(m)
            except KeyboardInterrupt:
                logger.info("Interrupted by user")
                break
            except Exception as e:
                logger.error(f"Unexpected loop error: {e}")
                time.sleep(5)
        logger.info("👋 Worker stopped")


# --------------------------- Dummy repo adapter -------------------------------
class DummyRepositoryAdapter:
    """
    Placeholder adapter for posting comments to PRs.
    Replace with your project's real GitHub/Azure adapter.
    """
    def comment_on_pr(self, pr_number: int, text: str) -> None:
        logger.info(f"(PR #{pr_number}) Comment preview:\n{text}")


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
