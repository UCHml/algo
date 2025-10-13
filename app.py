# --- INSERT: robustness helpers (graceful shutdown, dedup, small utils) ---
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




class SQSWorker:
    def __init__(self):
        self.sqs = boto3.client("sqs")  # <--- ЗАМЕНИ ЭТУ СТРОКУ
        self.queue_url = config.sqs_queue_url



# Explicit region to avoid client creating in wrong/default region
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



# --- INSERT at the very beginning of process_message() after 'try:' ---
# 1) Deduplicate to avoid re-analyzing same log repeatedly
dedup = _dedup_key(message)
if seen_recently(dedup):
    logger.info("↩️ Duplicate within TTL – skipping without analysis")
    return

# 2) Start visibility heartbeat so another worker does not pick the same message
receipt = message["ReceiptHandle"]
_done = [False]

def _extend_visibility():
    """Keep message invisible while we are processing."""
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




# --- INSERT near where you build prompt inputs ---
# Limit log size to save LLM tokens (pass as an extra variable to the prompt)
raw_log = body if isinstance(body, str) else json.dumps(body, ensure_ascii=False)
log_excerpt = trim_text(raw_log, max_chars=8000)
# when calling get_prompt(...), we already pass **prompt_pr_details; add log_excerpt too
prompt = get_prompt(
    payload.agent_id,
    **prompt_pr_details,
    repository_name=payload.repository_name,
    pr_number=payload.pr_id,
    claude_comment_id=comment_id,
    trigger_comment=payload.trigger_phrase or "No comment found",
    log_excerpt=log_excerpt,  # <--- ADD THIS PARAM for the prompt template
)
# --- END INSERT ---




# --- INSERT near the end of successful processing branch ---
# Save a compact report locally for demo/traceability
try:
    artifacts = Path("./artifacts")
    ensure_dir(artifacts)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    report_path = artifacts / f"{ts}_pr{payload.pr_id}.json"
    summary_payload = {
        "repository": payload.repository_name,
        "pr_id": payload.pr_id,
        "trigger": payload.trigger_phrase or None,
        "log_excerpt_len": len(log_excerpt or ""),
        "when_utc": ts,
        # Put here whatever summary you already produce from Claude/agent
        # e.g., prompt, top findings, next steps can be added if you have them as variables
    }
    report_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2))
    logger.info(f"📄 Report saved to {report_path}")
except Exception as _ex:
    logger.warning(f"Could not save local report: {_ex}")

# Mark as processed to avoid re-analysis if message appears again
mark_seen(dedup)

# Stop visibility heartbeat
_done[0] = True
# --- END INSERT ---




except Exception as e:
    _done[0] = True  # stop visibility heartbeat on failure as well
    logger.exception(f"Unexpected error processing message: {e}")
    # (если не удаляешь — пусть сообщение вернётся/упадёт в DLQ по политике)



while not SHUTDOWN:
