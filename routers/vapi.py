from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, BackgroundTasks, Request

from routers import agent, conversation_state, redis_state
from routers.agent import LEAD_HASH_KEY, _lead_to_redis_mapping

router = APIRouter()

STREAM_KEY = "meeting:transcript"

# Tracks how far we've processed conversation-update message arrays.
# Keyed by call/conversation id when available; falls back to "default".
_conversation_update_last_idx: dict[str, int] = {}


def _webhook_log_summary(body: dict) -> str:
    top = body.get("type") or "?"
    msg = body.get("message") if isinstance(body.get("message"), dict) else {}
    inner = (msg.get("type") or "?") if isinstance(msg, dict) else "?"
    call = body.get("call") if isinstance(body.get("call"), dict) else {}
    cid = (call.get("id") or "")[:12] if isinstance(call, dict) else ""
    tail = f" call.id={cid}…" if cid else ""
    return f"[VAPI] webhook top.type={top} message.type={inner}{tail}"


def _conversation_update_key(body: dict) -> str:
    call = body.get("call") or {}
    for candidate in (
        call.get("id"),
        body.get("callId"),
        body.get("call_id"),
        body.get("id"),
    ):
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return "default"


@router.get("/test")
async def vapi_test():
    length = await conversation_state.history_length()
    return {"history_length": length}


@router.post("/webhook")
async def vapi_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Vapi posts here for logging / dashboard sync. Always return 200 on success path so Vapi
    never treats the webhook as failed (some call flows are sensitive to non-2xx).
    Redis or SSE failures must not break the voice pipeline.
    """
    try:
        body = await request.json()
    except Exception:
        return {"status": "ok"}

    print(_webhook_log_summary(body))

    try:
        if body.get("type") == "call-started":
            print("[VAPI] call-started - resetting lead")
            try:
                r = redis_state.get_redis()
                defaults = {
                    "intent": "unknown",
                    "lead_score": 0,
                    "objections": [],
                    "pain_points": [],
                    "recommended_pitch": "",
                    "pipeline_stage": "cold",
                    "action": "none",
                    "demo_booked": False,
                    "followup_sent": False,
                }
                await asyncio.to_thread(r.hset, LEAD_HASH_KEY, mapping=_lead_to_redis_mapping(defaults))
            except Exception as e:
                print(f"[VAPI] call-started redis skipped: {e}", flush=True)
            return {"status": "ok"}

        speaker = ""
        text = ""
        is_partial = False

        msg = body.get("message") or {}

        # Case 0 — Vapi conversation-update (bulk array of turns)
        if msg.get("type") == "conversation-update":
            key = _conversation_update_key(body)
            messages = msg.get("messages") or []
            if not isinstance(messages, list):
                return {"status": "ok"}

            start = _conversation_update_last_idx.get(key, 0)
            if start < 0:
                start = 0
            for i in range(start, len(messages)):
                m = messages[i] or {}
                if not isinstance(m, dict):
                    continue
                if (m.get("role") or "") != "user":
                    continue
                t = (m.get("message") or "").strip()
                if not t:
                    continue

                print(f"[VAPI] transcript received - speaker: user text: {t[:500]}")

                try:
                    await conversation_state.append_chunk("user", t)
                except Exception as e:
                    print(f"[VAPI] append_chunk skip: {e}", flush=True)

                msg_id = ""
                try:
                    r = redis_state.get_redis()
                    msg_id = r.xadd(STREAM_KEY, {"speaker": "user", "text": t})
                except Exception as e:
                    print(f"[VAPI] redis xadd skip: {e}", flush=True)

                try:
                    await redis_state.sse_queue.put(
                        {
                            "type": "transcript",
                            "id": msg_id,
                            "speaker": "user",
                            "text": t,
                        }
                    )
                except Exception as e:
                    print(f"[VAPI] sse_queue skip: {e}", flush=True)

                asyncio.create_task(agent.enqueue_chunk("user", t))

            _conversation_update_last_idx[key] = len(messages)
            return {"status": "ok"}

        # Case 1 — Vapi native transcript (partial + final)
        if msg.get("type") == "transcript":
            transcript_type = (msg.get("transcriptType") or "").strip().lower()
            speaker = (msg.get("role") or "unknown").strip()
            text = (msg.get("transcript") or "").strip()
            is_partial = transcript_type == "partial"
        # Case 2 — manual curl / browser SDK echo format
        elif "speaker" in body and "text" in body:
            speaker = str(body.get("speaker") or "").strip()
            text = str(body.get("text") or "").strip()

        if text:
            if is_partial:
                try:
                    await redis_state.sse_queue.put(
                        {"action": "transcript_partial", "data": {"speaker": speaker, "text": text}}
                    )
                except Exception:
                    pass
                return {"status": "ok"}

            print(f"[VAPI] transcript received - speaker: {speaker} text: {text[:500]}")

            try:
                await conversation_state.append_chunk(speaker, text)
            except Exception as e:
                print(f"[VAPI] append_chunk skip: {e}", flush=True)

            msg_id = ""
            try:
                r = redis_state.get_redis()
                msg_id = r.xadd(STREAM_KEY, {"speaker": speaker, "text": text})
            except Exception as e:
                print(f"[VAPI] redis xadd skip: {e}", flush=True)

            try:
                await redis_state.sse_queue.put(
                    {
                        "type": "transcript",
                        "id": msg_id,
                        "speaker": speaker,
                        "text": text,
                    }
                )
            except Exception as e:
                print(f"[VAPI] sse_queue skip: {e}", flush=True)

            asyncio.create_task(agent.enqueue_chunk(speaker, text))

    except Exception as e:
        print(f"[VAPI] webhook handler error (still 200): {e}", flush=True)

    return {"status": "ok"}
