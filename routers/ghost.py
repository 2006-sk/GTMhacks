from __future__ import annotations

import html
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
import jwt
import redis
from dotenv import load_dotenv
from fastapi import APIRouter, Body, HTTPException
from pydantic import BaseModel, Field

from routers import redis_state

router = APIRouter()

_ROOT = Path(__file__).resolve().parent.parent


def _ghost_base() -> str:
    return (os.getenv("GHOST_API_URL") or "").rstrip("/")


def _content_key() -> str | None:
    k = os.getenv("GHOST_CONTENT_API_KEY")
    if not k or k == "your_ghost_content_api_key_here":
        return None
    return k


def _admin_key_parts() -> tuple[str, str]:
    raw = (os.getenv("GHOST_ADMIN_API_KEY") or "").strip()
    if not raw or ":" not in raw:
        raise HTTPException(status_code=503, detail="GHOST_ADMIN_API_KEY not set or invalid format (expected id:secret)")
    key_id, secret_hex = raw.split(":", 1)
    if not key_id or not secret_hex:
        raise HTTPException(status_code=503, detail="GHOST_ADMIN_API_KEY missing id or secret")
    return key_id, secret_hex


def _ghost_admin_jwt(key_id: str, secret_hex: str) -> str:
    try:
        secret = bytes.fromhex(secret_hex.strip())
    except ValueError as e:
        raise HTTPException(status_code=503, detail="GHOST_ADMIN_API_KEY secret is not valid hex") from e
    now = int(time.time())
    payload = {"iat": now, "exp": now + 300, "aud": "/admin/"}
    headers = {"alg": "HS256", "typ": "JWT", "kid": key_id}
    return jwt.encode(payload, secret, algorithm="HS256", headers=headers)


def _stream_id_display(msg_id: str) -> str:
    try:
        ms = int(str(msg_id).split("-", 1)[0])
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return str(msg_id)


def _fetch_all_hashes(r: redis.Redis, pattern: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for key in r.scan_iter(match=pattern):
        h = r.hgetall(key)
        if h:
            rows.append(h)
    return rows


def _build_recap_html(
    tasks: list[dict],
    decisions: list[dict],
    events: list[tuple[str, dict]],
    heading: str,
) -> str:
    n_events = len(events)

    def row_cells(cells: list[str]) -> str:
        return "<tr>" + "".join(f"<td>{html.escape(c)}</td>" for c in cells) + "</tr>"

    tasks_rows = (
        "".join(
            row_cells(
                [
                    t.get("title", ""),
                    t.get("owner", ""),
                    t.get("deadline", ""),
                    t.get("status", ""),
                ]
            )
            for t in tasks
        )
        or "<tr><td colspan=\"4\"><em>No tasks</em></td></tr>"
    )

    dec_rows = (
        "".join(
            row_cells(
                [
                    d.get("statement", ""),
                    d.get("speaker", ""),
                ]
            )
            for d in decisions
        )
        or "<tr><td colspan=\"2\"><em>No decisions</em></td></tr>"
    )

    timeline_rows: list[str] = []
    for msg_id, fields in events:
        raw = (fields.get("data") or "").strip()
        action = "?"
        try:
            obj = json.loads(raw)
            action = str(obj.get("action", "?"))
        except Exception:
            action = "parse_error"
        timeline_rows.append(
            row_cells([_stream_id_display(msg_id), html.escape(action), html.escape(raw[:500])])
        )
    timeline_body = "".join(timeline_rows) or "<tr><td colspan=\"3\"><em>No events</em></td></tr>"

    return f"""<article class="meetingmind-recap">
<h1>{html.escape(heading)}</h1>

<h2>Tasks</h2>
<table>
<thead><tr><th>Title</th><th>Owner</th><th>Deadline</th><th>Status</th></tr></thead>
<tbody>{tasks_rows}</tbody>
</table>

<h2>Decisions</h2>
<table>
<thead><tr><th>Statement</th><th>Speaker</th></tr></thead>
<tbody>{dec_rows}</tbody>
</table>

<h2>Event Timeline</h2>
<table>
<thead><tr><th>Time</th><th>Action</th><th>Payload (truncated)</th></tr></thead>
<tbody>{timeline_body}</tbody>
</table>

<p><strong>AI Insights:</strong> Agent processed {n_events} events autonomously.</p>
</article>"""


@router.get("/posts")
async def ghost_posts(limit: int = 5):
    load_dotenv(_ROOT / ".env", override=False)
    base = _ghost_base()
    key = _content_key()
    if not base or base == "https://your-site.ghost.io" or not key:
        raise HTTPException(status_code=503, detail="Ghost env not configured")
    url = f"{base}/ghost/api/content/posts/"
    params = {"key": key, "limit": limit}
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(url, params=params)
    if r.status_code >= 400:
        raise HTTPException(status_code=r.status_code, detail=r.text[:500])
    return r.json()


RECAP_SLUG = "meetingmind-os-meeting-recap"
# Marks recap posts so we can find them even when Ghost suffixes the slug (-2, -3, …).
RECAP_INTERNAL_TAG_SLUG = "hash-meetingmind-recap"
REDIS_RECAP_POST_ID_KEY = "meetingmind:ghost:recap_post_id"
# Ghost may create meetingmind-os-meeting-recap-2, -3, … on slug collision; OR-filter those.
_MAX_SLUG_SUFFIX_VARIANT = 60


def _recap_tags() -> list[dict[str, str]]:
    return [{"slug": RECAP_INTERNAL_TAG_SLUG}]


def _pick_canonical_recap_post(posts: list[dict]) -> dict | None:
    """Prefer exact RECAP_SLUG, else lowest numeric suffix recap-N (stable canonical row)."""
    if not posts:
        return None
    exact = [p for p in posts if (p.get("slug") or "") == RECAP_SLUG]
    if exact:
        return exact[0]
    prefix = RECAP_SLUG + "-"
    best: dict | None = None
    best_n = 10**9
    for p in posts:
        s = p.get("slug") or ""
        if not s.startswith(prefix):
            continue
        tail = s[len(prefix) :]
        if tail.isdigit():
            n = int(tail)
            if n < best_n:
                best_n = n
                best = p
    return best


def _slug_variants_filter() -> str:
    variants = [RECAP_SLUG] + [f"{RECAP_SLUG}-{i}" for i in range(2, _MAX_SLUG_SUFFIX_VARIANT + 1)]
    return "slug:[" + ",".join(variants) + "]"


async def _admin_read_post(
    client: httpx.AsyncClient,
    base: str,
    headers: dict[str, str],
    post_id: str,
) -> dict | None:
    resp = await client.get(f"{base}/ghost/api/admin/posts/{post_id}/", headers=headers)
    if resp.status_code == 404:
        return None
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text[:800])
    posts = (resp.json().get("posts") or [])[:1]
    return posts[0] if posts else None


async def _admin_browse_posts(
    client: httpx.AsyncClient,
    base: str,
    headers: dict[str, str],
    params: dict[str, str],
) -> list[dict]:
    resp = await client.get(f"{base}/ghost/api/admin/posts/", headers=headers, params=params)
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text[:800])
    return resp.json().get("posts") or []


async def _find_recap_post_for_upsert(
    client: httpx.AsyncClient,
    base: str,
    headers: dict[str, str],
    r: redis.Redis,
) -> dict | None:
    stored = (r.get(REDIS_RECAP_POST_ID_KEY) or "").strip()
    if stored:
        ep = await _admin_read_post(client, base, headers, stored)
        if ep and ep.get("id") and ep.get("updated_at"):
            return ep
        r.delete(REDIS_RECAP_POST_ID_KEY)

    posts = await _admin_browse_posts(client, base, headers, {"filter": f"slug:{RECAP_SLUG}", "limit": "5"})
    hit = _pick_canonical_recap_post(posts)
    if hit:
        return hit

    posts = await _admin_browse_posts(
        client,
        base,
        headers,
        {"filter": _slug_variants_filter(), "limit": str(_MAX_SLUG_SUFFIX_VARIANT)},
    )
    hit = _pick_canonical_recap_post(posts)
    if hit:
        return hit

    posts = await _admin_browse_posts(
        client,
        base,
        headers,
        {"filter": f"tag:{RECAP_INTERNAL_TAG_SLUG}", "limit": "50"},
    )
    return _pick_canonical_recap_post(posts)


class GhostPublishOptions(BaseModel):
    """Optional overrides for recap content (slug is fixed for upsert)."""

    title: str | None = Field(default=None, max_length=255)
    heading: str | None = Field(default=None, max_length=300)


@router.post("/publish")
async def ghost_publish(body: GhostPublishOptions | None = Body(default=None)):
    """
    Build a recap post from Redis (tasks, decisions, meeting:events) and upsert via Ghost Admin API:
    same slug plus internal tag, Redis-stored post id, and slug-variant browse so Ghost’s -2/-3
    suffixes still resolve to one post.
    """
    load_dotenv(_ROOT / ".env", override=False)
    base = _ghost_base()
    if not base or base == "https://your-site.ghost.io":
        raise HTTPException(status_code=503, detail="GHOST_API_URL not configured")

    key_id, secret_hex = _admin_key_parts()
    token = _ghost_admin_jwt(key_id, secret_hex)

    await redis_state.sse_queue.put({"action": "ghost_status", "data": {"status": "publishing"}})

    try:
        r = redis_state.get_redis()
    except RuntimeError as e:
        await redis_state.sse_queue.put({"action": "ghost_status", "data": {"status": "failed", "detail": str(e)}})
        raise HTTPException(status_code=503, detail=str(e)) from e

    tasks = _fetch_all_hashes(r, "task:*")
    decisions = _fetch_all_hashes(r, "decision:*")
    try:
        events = r.xrange("meeting:events", "-", "+") if r.exists("meeting:events") else []
    except redis.RedisError:
        events = []
    opts = body or GhostPublishOptions()
    default_title = "MeetingMind OS — Meeting Recap"
    title = (opts.title or default_title).strip()
    heading = (opts.heading or title).strip()
    html_body = _build_recap_html(tasks, decisions, events, heading=heading)

    headers = {
        "Authorization": f"Ghost {token}",
        "Content-Type": "application/json",
        "Accept-Version": "v5.0",
    }

    def _post_url_from_response(data: dict) -> str:
        posts_out = data.get("posts") or []
        p0 = posts_out[0] if posts_out else {}
        return (p0.get("url") or "").strip()

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            try:
                ep = await _find_recap_post_for_upsert(client, base, headers, r)
            except HTTPException as browse_err:
                await redis_state.sse_queue.put(
                    {
                        "action": "ghost_status",
                        "data": {"status": "failed", "detail": (browse_err.detail or str(browse_err))[:500]},
                    }
                )
                raise

            if ep:
                post_id = ep.get("id")
                updated_at = ep.get("updated_at")
                if not post_id or not updated_at:
                    await redis_state.sse_queue.put(
                        {
                            "action": "ghost_status",
                            "data": {"status": "failed", "detail": "Existing post missing id or updated_at"},
                        }
                    )
                    raise HTTPException(status_code=502, detail="Ghost browse response missing id/updated_at")

                put_url = f"{base}/ghost/api/admin/posts/{post_id}/?source=html"
                put_body = {
                    "posts": [
                        {
                            "id": post_id,
                            "updated_at": updated_at,
                            "title": title,
                            "html": html_body,
                            "slug": RECAP_SLUG,
                            "status": "published",
                            "email_only": False,
                            "tags": _recap_tags(),
                        }
                    ]
                }
                resp = await client.put(put_url, json=put_body, headers=headers)
            else:
                create_url = f"{base}/ghost/api/admin/posts/?source=html"
                create_body = {
                    "posts": [
                        {
                            "title": title,
                            "html": html_body,
                            "slug": RECAP_SLUG,
                            "status": "published",
                            "email_only": False,
                            "tags": _recap_tags(),
                        }
                    ]
                }
                resp = await client.post(create_url, json=create_body, headers=headers)
    except HTTPException:
        raise
    except Exception as e:
        await redis_state.sse_queue.put({"action": "ghost_status", "data": {"status": "failed", "detail": str(e)}})
        raise HTTPException(status_code=502, detail=str(e)) from e

    if resp.status_code >= 400:
        await redis_state.sse_queue.put(
            {"action": "ghost_status", "data": {"status": "failed", "detail": resp.text[:500]}}
        )
        raise HTTPException(status_code=resp.status_code, detail=resp.text[:800])

    try:
        data = resp.json()
        post_url = _post_url_from_response(data)
        posts_out = data.get("posts") or []
        new_id = (posts_out[0].get("id") if posts_out else None) or None
        if new_id:
            r.set(REDIS_RECAP_POST_ID_KEY, str(new_id))
    except Exception:
        post_url = ""

    if not post_url:
        await redis_state.sse_queue.put({"action": "ghost_status", "data": {"status": "failed", "detail": "No url in Ghost response"}})
        raise HTTPException(status_code=502, detail="Ghost response missing post URL")

    await redis_state.sse_queue.put({"action": "ghost_status", "data": {"status": "delivered", "url": post_url}})
    return {"url": post_url}
