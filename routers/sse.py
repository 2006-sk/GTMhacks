import asyncio
import json

from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import StreamingResponse

from routers import redis_state
from routers.redis_state import sse_queue
from routers.vapi import STREAM_KEY as TRANSCRIPT_STREAM

REPLAY_SLEEP_S = 0.8


async def _replay_streams_to_sse_queue() -> None:
    await sse_queue.put({"action": "replay_start", "data": {}})

    def _load_streams() -> tuple[list[tuple[str, dict[str, str]]], list[tuple[str, dict[str, str]]]]:
        try:
            r = redis_state.get_redis()
        except RuntimeError:
            return [], []
        events: list[tuple[str, dict[str, str]]] = (
            r.xrange(redis_state.MEETING_EVENTS_STREAM, "-", "+")
            if r.exists(redis_state.MEETING_EVENTS_STREAM)
            else []
        )
        transcripts: list[tuple[str, dict[str, str]]] = (
            r.xrange(TRANSCRIPT_STREAM, "-", "+") if r.exists(TRANSCRIPT_STREAM) else []
        )
        return events, transcripts

    events, transcripts = await asyncio.to_thread(_load_streams)

    merged: list[tuple[str, str, dict[str, str]]] = []
    for msg_id, fields in events:
        merged.append((msg_id, "events", fields))
    for msg_id, fields in transcripts:
        merged.append((msg_id, "transcript", fields))
    merged.sort(key=lambda row: row[0])

    for i, (msg_id, kind, fields) in enumerate(merged):
        if i > 0:
            await asyncio.sleep(REPLAY_SLEEP_S)
        if kind == "events":
            await sse_queue.put(redis_state._parse_event(fields))
        else:
            await sse_queue.put(
                {
                    "type": "transcript",
                    "id": msg_id,
                    "speaker": fields.get("speaker", ""),
                    "text": fields.get("text", ""),
                }
            )

    await sse_queue.put({"action": "replay_end", "data": {}})


router = APIRouter()


@router.get("/stream")
async def sse_stream():
    async def event_gen():
        try:
            while True:
                event = await sse_queue.get()
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
        except asyncio.CancelledError:
            # Client disconnected / request cancelled.
            return

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/test")
async def sse_test():
    await sse_queue.put({"type": "test", "ok": True})
    return {"ok": True}


def _fetch_all_hashes_by_pattern(pattern: str) -> list[dict[str, str]]:
    try:
        r = redis_state.get_redis()
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    rows: list[dict[str, str]] = []
    for key in r.scan_iter(match=pattern):
        h = r.hgetall(key)
        if h:
            rows.append(h)
    return rows


@router.get("/tasks")
async def sse_tasks():
    """Return all Redis task:* hashes as a JSON array."""
    return await asyncio.to_thread(_fetch_all_hashes_by_pattern, "task:*")


@router.get("/decisions")
async def sse_decisions():
    """Return all Redis decision:* hashes as a JSON array."""
    return await asyncio.to_thread(_fetch_all_hashes_by_pattern, "decision:*")


@router.get("/replay")
async def sse_replay(background_tasks: BackgroundTasks):
    """
    Enqueue a chronological replay of meeting:events and meeting:transcript onto sse_queue
    (see GET /sse/stream). Runs in a background task so this returns immediately.
    """
    background_tasks.add_task(_replay_streams_to_sse_queue)
    return {"status": "started"}
