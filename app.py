# core/llm_analyzer.py
# --------------------
# Purpose: Compact LLM-based incident analysis for deployment logs.
# - Trims input to save tokens
# - Extracts key errors first (cheap heuristics)
# - Calls Anthropic Claude with strict JSON schema
# - Retries + validates with Pydantic

from __future__ import annotations
import os, re, json, time, hashlib
from typing import List, Optional, Literal
from pydantic import BaseModel, Field, ValidationError
from anthropic import Anthropic, APIError, RateLimitError

# ---------- Config ----------
ANTHROPIC_KEY = os.getenv("ANTHROPIC_AUTH_TOKEN")
MODEL = os.getenv("ANTHROPIC_MODEL", "claude-3-haiku-20240307")  # cost-friendly default
MAX_OUTPUT_TOKENS = int(os.getenv("LLM_MAX_OUTPUT_TOKENS", "600"))
TEMP = float(os.getenv("LLM_TEMPERATURE", "0"))

# ---------- Report Schema ----------
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

# ---------- Cheap heuristics (pre-LLM) ----------
ERROR_PAT = re.compile(r"(?i)\b(error|exception|failed|traceback|crash|panic|fatal)\b")
WARN_PAT  = re.compile(r"(?i)\b(warn|warning|timeout|throttle)\b")

def build_log_digest(raw: str, max_chars: int = 8000) -> str:
    """Build a compact digest: tail + extracted error/warn lines. Saves tokens."""
    if not raw:
        return ""
    lines = raw.splitlines()
    errors = [l for l in lines if ERROR_PAT.search(l)]
    warns  = [l for l in lines if WARN_PAT.search(l)]
    # keep tail to preserve recent context
    tail = "\n".join(lines[-500:])  # ~ recent 500 lines
    # merge with dedup
    merged = []
    seen  = set()
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

# ---------- LLM Call ----------
SYS_PROMPT = """You are a senior incident analyst for CI/CD and cloud deployments.
You read raw logs and produce a crisp, actionable incident report.
Be concise, technical, and specific. If uncertain, say so explicitly."""

USER_TMPL = """Analyze the following deployment/runtime logs and return ONLY valid JSON following the schema."""

#Context (optional):
#- repo: {repo}
#- pr_id: {pr_id}
#- agent_id: {agent_id}

#Logs (digest):



def _client() -> Anthropic:
    if not ANTHROPIC_KEY:
        raise RuntimeError("ANTHROPIC_AUTH_TOKEN is missing")
    return Anthropic(api_key=ANTHROPIC_KEY)

def analyze_logs_with_llm(digest: str, repo: str, pr_id: int, agent_id: str, retries: int = 3, backoff: float = 1.5) -> Report:
    """Call Claude and validate JSON output. Retries on transient errors."""
    payload = USER_TMPL.format(
        repo=repo, pr_id=pr_id, agent_id=agent_id,
        digest=digest,
        schema=json.dumps(Report.model_json_schema(), ensure_ascii=False, indent=2),
    )
    client = _client()
    last_err: Optional[Exception] = None

    for attempt in range(1, retries + 1):
        try:
            resp = client.messages.create(
                model=MODEL,
                temperature=TEMP,
                max_tokens=MAX_OUTPUT_TOKENS,
                messages=[
                    {"role": "system", "content": SYS_PROMPT},
                    {"role": "user",   "content": payload},
                ]
            )
            text = "".join([c.text for c in resp.content if getattr(c, "type", None) == "text"])  # robust extraction
            data = json.loads(text)
            report = Report.model_validate(data)
            return report
        except (RateLimitError, APIError) as e:
            last_err = e
            time.sleep(backoff ** attempt)
        except (json.JSONDecodeError, ValidationError) as e:
            # Ask model to fix to strict JSON on next attempt by appending a reminder
            last_err = e
            payload = payload + "\n\nREMINDER: Output must be STRICT JSON only. No markdown, no explanations."
            time.sleep(backoff ** attempt)

    raise RuntimeError(f"LLM analysis failed after {retries} attempts: {last_err}")










