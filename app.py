# --- INSERT: robustness helpers (graceful shutdown, dedup, utils) ---
import os
import signal
import threading
import hashlib
from datetime import datetime, timedelta
from pathlib import Path

# Global shutdown flag to stop loops gracefully
SHUTDOWN = False

# In-memory idempotency store to avoid re-processing same message within TTL
_SEEN = {}
_SEEN_TTL_MIN = int(os.getenv("DEDUPLICATION_MINUTES", "10"))
_SEEN_LOCK = threading.Lock()

def _dedup_key(message: dict) -> str:
    """Create a stable deduplication key from MessageId+Body"""
    mid = message.get("MessageId", "")
    body = message.get("Body", "")
    return hashlib.sha256((mid + "|" + body).encode()).hexdigest()

def seen_recently(key: str) -> bool:
    """Return True if message was processed recently (within TTL)"""
    with _SEEN_LOCK:
        exp = _SEEN.get(key)
        if not exp:
            return False
        if datetime.utcnow() > exp:
            del _SEEN[key]
            return False
        return True

def mark_seen(key: str):
    """Mark message as processed for TTL minutes"""
    with _SEEN_LOCK:
        _SEEN[key] = datetime.utcnow() + timedelta(minutes=_SEEN_TTL_MIN)

def handle_shutdown(*_):
    """SIGINT/SIGTERM handler"""
    global SHUTDOWN
    SHUTDOWN = True

# Register graceful shutdown handlers
signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def trim_text(text: str, max_chars: int = 8000) -> str:
    """Keep only last N chars to save LLM tokens"""
    if not text:
        return text
    return text[-max_chars:]
# --- END INSERT ---






# --- INSERT: LLM analyzer (strict JSON incident report) ---
from core.llm_analyzer import build_log_digest, analyze_logs_with_llm, Report
# --- END INSERT ---





class SQSWorker:
    def __init__(self):
        self.sqs = boto3.client("sqs")   # <-- REPLACE THIS LINE
        self.queue_url = config.sqs_queue_url
# Explicit region to avoid wrong/default region
self.sqs = boto3.client("sqs", region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))





def poll_messages(self) -> list:
    """Long-poll SQS for up to 5 messages (saves CPU and API calls)"""
    try:
        response = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=5,      # batch read
            WaitTimeSeconds=20,         # long poll
            MessageAttributeNames=["All"]
        )
        messages = response.get("Messages", [])
        return messages
    except ClientError as e:
        logger.error(f"Error polling SQS: {e}")
        time.sleep(5)  # small backoff
        return []




# --- INSERT at the beginning of process_message() after 'try:' ---
# 1) Deduplicate to avoid re-analyzing same log repeatedly
dedup = _dedup_key(message)
if seen_recently(dedup):
    logger.info("↩️ Duplicate within TTL – skipping without analysis")
    return

# 2) Start visibility heartbeat so another worker does not pick the same message
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
# --- END INSERT ---




except Exception as e:
    _done[0] = True  # stop visibility heartbeat on failure
    logger.exception(f"Unexpected error processing message: {e}")
    return



# --- INSERT: build digest and call LLM for strict JSON report ---
raw_log = body if isinstance(body, str) else json.dumps(body, ensure_ascii=False)
digest = build_log_digest(raw_log, max_chars=8000)

try:
    report: Report = analyze_logs_with_llm(
        digest=digest,
        repo=payload.repository_name,
        pr_id=payload.pr_id,
        agent_id=str(payload.agent_id),
    )
    logger.info("🧠 LLM report assembled")
except Exception as e:
    logger.warning(f"LLM analysis failed, using fallback: {e}")
    # Fallback minimal report (no LLM)
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
# --- END INSERT ---




# --- INSERT near the end of successful processing branch ---
# Persist JSON report locally for demo/tracing
ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
artifacts = Path("./artifacts"); ensure_dir(artifacts)
out = artifacts / f"{ts}_pr{payload.pr_id}_report.json"
out.write_text(report.model_dump_json(indent=2, ensure_ascii=False))
logger.info(f"📄 Report saved: {out}")

# (Optional) Post a short summary to PR (adjust to your adapter)
short = (
    f"**Deployment Incident Report (auto)**\n"
    f"- Severity: `{report.severity}`\n"
    f"- Summary: {report.summary}\n"
    f"- Next steps: " + "; ".join(report.next_steps[:3])
)
try:
    repository.comment_on_pr(payload.pr_id, short)  # replace with your adapter/method if name differs
except Exception as ex:
    logger.warning(f"PR comment failed: {ex}")

# Mark as processed to avoid re-analysis on re-delivery
mark_seen(dedup)

# Stop visibility heartbeat
_done[0] = True
# --- END INSERT ---




# --- INSERT near the end of successful processing branch ---
# Persist JSON report locally for demo/tracing
ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
artifacts = Path("./artifacts"); ensure_dir(artifacts)
out = artifacts / f"{ts}_pr{payload.pr_id}_report.json"
out.write_text(report.model_dump_json(indent=2, ensure_ascii=False))
logger.info(f"📄 Report saved: {out}")

# (Optional) Post a short summary to PR (adjust to your adapter)
short = (
    f"**Deployment Incident Report (auto)**\n"
    f"- Severity: `{report.severity}`\n"
    f"- Summary: {report.summary}\n"
    f"- Next steps: " + "; ".join(report.next_steps[:3])
)
try:
    repository.comment_on_pr(payload.pr_id, short)  # replace with your adapter/method if name differs
except Exception as ex:
    logger.warning(f"PR comment failed: {ex}")

# Mark as processed to avoid re-analysis on re-delivery
mark_seen(dedup)

# Stop visibility heartbeat
_done[0] = True
# --- END INSERT ---




while not SHUTDOWN:




