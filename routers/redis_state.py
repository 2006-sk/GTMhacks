from __future__ import annotations

import asyncio
import json
import os
from typing import Any

import redis
from fastapi import APIRouter, HTTPException

router = APIRouter()

_pool: redis.ConnectionPool | None = None

# Exported for SSE streaming
sse_queue: asyncio.Queue[dict] = asyncio.Queue()

MEETING_EVENTS_STREAM = "meeting:events"
STATE_UPDATER_GROUP = "state-updater"
SSE_PUSHER_GROUP = "sse-pusher"
CONSUMER_NAME = "meetingmind"


def init_redis_pool() -> None:
    """Create a shared connection pool from env. Safe to call once at app startup."""
    global _pool
    _pool = None
    host = os.getenv("REDIS_HOST")
    password = os.getenv("REDIS_PASSWORD")
    port_raw = os.getenv("REDIS_PORT", "6379")
    if not host or not password or host == "your_redis_host_here":
        return
    try:
        port = int(port_raw)
    except ValueError:
        return
    _pool = redis.ConnectionPool(
        host=host,
        port=port,
        password=password,
        decode_responses=True,
        max_connections=32,
    )


def close_redis_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.disconnect()
        _pool = None


def get_redis() -> redis.Redis:
    if _pool is None:
        raise RuntimeError("Redis pool is not initialized or env is incomplete")
    return redis.Redis(connection_pool=_pool)


async def _ensure_consumer_groups() -> None:
    """Create consumer groups if they don't exist (ignore BUSYGROUP)."""
    r = get_redis()

    def _create(group: str) -> None:
        try:
            r.xgroup_create(name=MEETING_EVENTS_STREAM, groupname=group, id="0-0", mkstream=True)
        except redis.ResponseError as e:
            # BUSYGROUP Consumer Group name already exists
            if "BUSYGROUP" not in str(e):
                raise

    await asyncio.to_thread(_create, STATE_UPDATER_GROUP)
    await asyncio.to_thread(_create, SSE_PUSHER_GROUP)


def _parse_event(fields: dict[str, str]) -> dict:
    raw = (fields.get("data") or "").strip()
    if raw.startswith("```"):
        # Drop leading ``` or ```json
        first_nl = raw.find("\n")
        if first_nl != -1:
            raw = raw[first_nl + 1 :].strip()
        if raw.endswith("```"):
            raw = raw[:-3].strip()
    if not raw:
        return {"action": "none", "data": {}}
    try:
        return json.loads(raw)
    except Exception:
        # Never crash workers on malformed events; pass raw through.
        return {"action": "none", "data": {}, "_raw": raw}


async def state_updater_worker() -> None:
    """
    Consumer 1 — State Updater:
    Reads meeting:events via XREADGROUP(state-updater/meetingmind) and applies diffs into Redis hashes.
    """
    await _ensure_consumer_groups()
    r = get_redis()

    while True:
        try:
            resp = await asyncio.to_thread(
                r.xreadgroup,
                STATE_UPDATER_GROUP,
                CONSUMER_NAME,
                {MEETING_EVENTS_STREAM: ">"},
                10,
                0,
            )
            if resp:
                # resp: [(stream, [(id, {field: value}) ...])]
                for _stream, messages in resp:
                    for msg_id, fields in messages:
                        try:
                            evt = _parse_event(fields)
                            action = evt.get("action")
                            data = evt.get("data") or {}

                            if action == "task_created":
                                task_id = data.get("id")
                                if task_id:
                                    key = f"task:{task_id}"
                                    await asyncio.to_thread(r.hset, key, mapping=data)
                            elif action == "task_updated":
                                task_id = data.get("id")
                                if task_id:
                                    key = f"task:{task_id}"
                                    changed = {k: v for k, v in data.items() if k != "id"}
                                    if changed:
                                        await asyncio.to_thread(r.hset, key, mapping=changed)
                            elif action == "decision_made":
                                dec_id = data.get("id")
                                if dec_id:
                                    key = f"decision:{dec_id}"
                                    await asyncio.to_thread(r.hset, key, mapping=data)
                            elif action == "conflict_detected":
                                task_id = data.get("task_id")
                                if task_id:
                                    key = f"task:{task_id}"
                                    await asyncio.to_thread(r.hset, key, mapping={"conflict": json.dumps(data)})
                            elif action == "conflict_resolved":
                                task_id = data.get("task_id")
                                if task_id:
                                    key = f"task:{task_id}"
                                    mapping: dict[str, str] = {"status": "confirmed"}
                                    if "resolved_value" in data and data["resolved_value"] is not None:
                                        mapping["deadline"] = str(data["resolved_value"])
                                    await asyncio.to_thread(r.hset, key, mapping=mapping)
                                    await asyncio.to_thread(r.hdel, key, "conflict")
                            elif action == "risk_flagged":
                                task_id = data.get("task_id")
                                if task_id:
                                    key = f"task:{task_id}"
                                    await asyncio.to_thread(r.hset, key, mapping={"risk": json.dumps(data)})

                        finally:
                            await asyncio.to_thread(r.xack, MEETING_EVENTS_STREAM, STATE_UPDATER_GROUP, msg_id)
        except redis.ResponseError as e:
            # Stream/group might have been deleted/reset; recreate and continue.
            if "NOGROUP" in str(e):
                try:
                    await _ensure_consumer_groups()
                except Exception:
                    pass
            else:
                pass
        except Exception:
            # keep looping
            pass
        await asyncio.sleep(0.1)


async def sse_pusher_worker() -> None:
    """
    Consumer 2 — SSE Pusher:
    Reads meeting:events via XREADGROUP(sse-pusher/meetingmind), enqueues parsed events to sse_queue.
    """
    await _ensure_consumer_groups()
    r = get_redis()

    while True:
        try:
            resp = await asyncio.to_thread(
                r.xreadgroup,
                SSE_PUSHER_GROUP,
                CONSUMER_NAME,
                {MEETING_EVENTS_STREAM: ">"},
                10,
                0,
            )
            if resp:
                for _stream, messages in resp:
                    for msg_id, fields in messages:
                        try:
                            evt = _parse_event(fields)
                            await sse_queue.put(evt)
                        finally:
                            await asyncio.to_thread(r.xack, MEETING_EVENTS_STREAM, SSE_PUSHER_GROUP, msg_id)
        except redis.ResponseError as e:
            if "NOGROUP" in str(e):
                try:
                    await _ensure_consumer_groups()
                except Exception:
                    pass
            else:
                pass
        except Exception:
            pass
        await asyncio.sleep(0.1)

@router.get("/ping")
def redis_ping():
    try:
        r = get_redis()
    except RuntimeError:
        raise HTTPException(status_code=503, detail="Redis env not configured") from None
    try:
        return {"pong": bool(r.ping())}
    except redis.RedisError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e


@router.get("/get/{key:path}")
def redis_get(key: str) -> dict[str, Any]:
    try:
        r = get_redis()
    except RuntimeError:
        raise HTTPException(status_code=503, detail="Redis env not configured") from None
    try:
        t = (r.type(key) or "").lower()
        if t == "hash":
            return {"key": key, "type": "hash", "value": r.hgetall(key)}
        if t in {"string", ""}:
            return {"key": key, "type": "string" if t else "none", "value": r.get(key)}
        # Fallback for other Redis types (list/set/zset/stream, etc.)
        return {"key": key, "type": t or "unknown", "value": None}
    except redis.RedisError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e


@router.post("/flush")
def redis_flush_meetingmind() -> dict[str, Any]:
    """
    Wipe MeetingMind keys only (safe alternative to FLUSHALL).
    Removes:
    - task:* hashes
    - decision:* hashes
    - meeting:events stream
    - meeting:transcript stream
    - meetingmind:ghost:recap_post_id
    """
    try:
        r = get_redis()
    except RuntimeError:
        raise HTTPException(status_code=503, detail="Redis env not configured") from None

    patterns = ["task:*", "decision:*"]
    explicit_keys = [
        "meeting:events",
        "meeting:transcript",
        "meetingmind:ghost:recap_post_id",
        "lead:current",
        "followup:email",
    ]

    keys: list[str] = []
    for pat in patterns:
        keys.extend(list(r.scan_iter(match=pat)))
    for k in explicit_keys:
        if r.exists(k):
            keys.append(k)
    # de-dupe while preserving order
    keys = list(dict.fromkeys(keys))

    deleted = 0
    if keys:
        deleted = int(r.delete(*keys))

    return {"status": "ok", "deleted": deleted, "keys": keys[:50], "keys_truncated": len(keys) > 50}
