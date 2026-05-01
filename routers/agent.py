from __future__ import annotations

import json
import os
from pathlib import Path

import asyncio
import redis
from fastapi import APIRouter
from groq import AsyncGroq
from dotenv import load_dotenv

from routers import conversation_state, redis_state
from routers.redis_state import sse_queue

router = APIRouter()

_ROOT = Path(__file__).resolve().parent.parent

_agent_queue: "asyncio.Queue[tuple[str, str]]" = asyncio.Queue()

SYSTEM_PROMPT = (
    "You are the backend lead-scoring analyst for Callbook.ai. Analyze the newest transcript chunk using the full "
    "conversation history, then update the lead intelligence for the sales dashboard. Callbook.ai's autonomous GTM "
    "agent finds companies with expensive, repetitive, high-volume outbound calling workflows where missed contacts, "
    "slow follow-up, and manual call-center labor create measurable revenue leakage. Strong-fit accounts are "
    "collections-heavy and CRM-driven organizations, especially lenders, fintechs, loan servicers, healthcare "
    "revenue-cycle teams, education providers, insurance companies, and appointment-based businesses with large "
    "past-due, stale, or unconverted contact lists. Important trigger signals include collections hiring, SDR or "
    "call-center hiring, Zoho CRM usage, delinquency exposure, revenue-cycle staffing pressure, new market expansion, "
    "and customer complaints about missed calls or poor follow-up. Score fit based on likely buyer seniority, trigger "
    "evidence, outbound call/list volume, revenue leakage, CRM readiness, follow-up urgency, compliance complexity, "
    "and timeline. Recommended pitch should position Callbook.ai as a fast-to-deploy, multichannel AI voice platform "
    "for turning large contact portfolios into live conversations with CRM-synced outcomes. Return ONLY raw JSON with "
    "these exact fields: intent (high/medium/low/unknown), lead_score (integer 0-100), objections (string array), "
    "pain_points (string array), recommended_pitch (string), pipeline_stage (cold/interested/qualified/demo_ready), "
    "action (none/book_demo/send_followup/escalate). No markdown, no backticks, raw JSON only."
)

LEAD_HASH_KEY = "lead:current"


def _strip_jsonish(text: str) -> str:
    s = text.strip()
    if s.startswith("```"):
        # Drop leading ``` or ```json
        first_nl = s.find("\n")
        if first_nl != -1:
            s = s[first_nl + 1 :]
        s = s.strip()
        if s.endswith("```"):
            s = s[: -3].strip()
    return s.strip()


def _redis_hash_to_lead(h: dict[str, str]) -> dict:
    def parse_json_array(v: str | None) -> list[str]:
        if not v:
            return []
        try:
            arr = json.loads(v)
            return [str(x) for x in arr] if isinstance(arr, list) else []
        except Exception:
            return []

    def parse_bool(v: str | None) -> bool:
        if v is None:
            return False
        return str(v).strip().lower() in {"1", "true", "yes", "y", "t"}

    lead_score_raw = (h.get("lead_score") or "").strip()
    try:
        lead_score = int(float(lead_score_raw)) if lead_score_raw else 0
    except Exception:
        lead_score = 0

    return {
        "intent": (h.get("intent") or "unknown").strip() or "unknown",
        "lead_score": max(0, min(100, lead_score)),
        "objections": parse_json_array(h.get("objections")),
        "pain_points": parse_json_array(h.get("pain_points")),
        "recommended_pitch": (h.get("recommended_pitch") or "").strip(),
        "pipeline_stage": (h.get("pipeline_stage") or "cold").strip() or "cold",
        "action": (h.get("action") or "none").strip() or "none",
        "demo_booked": parse_bool(h.get("demo_booked")),
        "followup_sent": parse_bool(h.get("followup_sent")),
    }


def _lead_to_redis_mapping(lead: dict) -> dict[str, str]:
    return {
        "intent": str(lead.get("intent") or "unknown"),
        "lead_score": str(int(lead.get("lead_score") or 0)),
        "objections": json.dumps(list(lead.get("objections") or []), ensure_ascii=False),
        "pain_points": json.dumps(list(lead.get("pain_points") or []), ensure_ascii=False),
        "recommended_pitch": str(lead.get("recommended_pitch") or ""),
        "pipeline_stage": str(lead.get("pipeline_stage") or "cold"),
        "action": str(lead.get("action") or "none"),
        "demo_booked": "true" if bool(lead.get("demo_booked")) else "false",
        "followup_sent": "true" if bool(lead.get("followup_sent")) else "false",
    }


async def process_chunk(speaker: str, text: str) -> dict:
    """
    Called after a transcript chunk is persisted.
    Calls Groq, stores the resulting lead JSON into Redis hash lead:current,
    emits an SSE lead_update event, and returns the lead object.
    """
    r = redis_state.get_redis()

    # Ensure local changes to .env are picked up even without a server restart.
    load_dotenv(_ROOT / ".env", override=False)

    history = await conversation_state.history_snapshot()
    print(f"[GROQ] calling with {len(history)} messages in history")

    user_message = json.dumps(
        {
            "new_chunk": {"speaker": speaker, "text": text},
            "conversation_history": history,
        },
        ensure_ascii=False,
    )

    api_key = (os.getenv("GROQ_API_KEY") or "").strip()
    if not api_key or api_key == "your_groq_api_key_here":
        lead = _redis_hash_to_lead(r.hgetall(LEAD_HASH_KEY) or {})
        print("[GROQ] skipped — no API key; pushing cached lead")
        print(f"[SSE] pushing lead_update score={lead.get('lead_score')}")
        await sse_queue.put({"type": "lead_update", "lead": lead})
        return lead

    try:
        client = AsyncGroq(api_key=api_key)
        completion = await client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_message},
            ],
            temperature=0.0,
            response_format={"type": "json_object"},
        )
        raw_text = (completion.choices[0].message.content or "").strip()
    except Exception as e:
        print(f"[GROQ] error: {e}")
        lead = _redis_hash_to_lead(r.hgetall(LEAD_HASH_KEY) or {})
        print(f"[SSE] pushing lead_update score={lead.get('lead_score')}")
        await sse_queue.put({"type": "lead_update", "lead": lead})
        return lead

    cleaned = _strip_jsonish(raw_text)
    parsed = json.loads(cleaned) if cleaned else {}
    if not isinstance(parsed, dict):
        parsed = {}

    existing = _redis_hash_to_lead(r.hgetall(LEAD_HASH_KEY) or {})
    lead = {
        **existing,
        **{
            "intent": parsed.get("intent", existing.get("intent", "unknown")),
            "lead_score": parsed.get("lead_score", existing.get("lead_score", 0)),
            "objections": parsed.get("objections", existing.get("objections", [])),
            "pain_points": parsed.get("pain_points", existing.get("pain_points", [])),
            "recommended_pitch": parsed.get("recommended_pitch", existing.get("recommended_pitch", "")),
            "pipeline_stage": parsed.get("pipeline_stage", existing.get("pipeline_stage", "cold")),
            "action": parsed.get("action", existing.get("action", "none")),
        },
    }

    # Normalize types & bounds by round-tripping through Redis mapping parser
    lead = _redis_hash_to_lead(_lead_to_redis_mapping(lead))

    print(f"[GROQ] response received: {json.dumps(lead, ensure_ascii=False)}")
    await asyncio.to_thread(r.hset, LEAD_HASH_KEY, mapping=_lead_to_redis_mapping(lead))
    print(f"[SSE] pushing lead_update score={lead.get('lead_score')}")
    await sse_queue.put({"type": "lead_update", "lead": lead})
    return lead


async def enqueue_chunk(speaker: str, text: str) -> None:
    """Fast enqueue used by webhook; processed sequentially by agent_worker()."""
    print(f"[AGENT] chunk enqueued speaker={speaker!r} text={text[:200]!r}...")
    await _agent_queue.put((speaker, text))


async def agent_worker() -> None:
    """Sequentially processes transcript chunks so meeting:events ordering matches webhook ordering."""
    while True:
        try:
            speaker, text = await _agent_queue.get()
            try:
                await process_chunk(speaker, text)
            finally:
                _agent_queue.task_done()
        except asyncio.CancelledError:
            return
        except Exception:
            # swallow and continue
            await asyncio.sleep(0.1)


@router.get("/status")
def agent_status():
    return {"agent": "ok"}
