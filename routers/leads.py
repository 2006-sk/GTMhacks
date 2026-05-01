from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import httpx
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException
from groq import AsyncGroq
from sse_starlette.sse import EventSourceResponse

router = APIRouter()

_ROOT = Path(__file__).resolve().parent.parent

leads_sse_queue: asyncio.Queue[dict] = asyncio.Queue()

APIFY_ACTOR_SYNC_URL = (
    "https://api.apify.com/v2/acts/apify~google-search-scraper/run-sync-get-dataset-items"
)

DEFAULT_SCRAPER_INPUT = {
    "queries": "debt collection software lending fintech USA",
    "maxPagesPerQuery": 1,
    "resultsPerPage": 10,
}


def _strip_jsonish(text: str) -> str:
    s = text.strip()
    if s.startswith("```"):
        first_nl = s.find("\n")
        if first_nl != -1:
            s = s[first_nl + 1 :]
        s = s.strip()
        if s.endswith("```"):
            s = s[: -3].strip()
    return s.strip()


def _clean_title(title: str) -> str:
    s = (title or "").strip()
    if not s:
        return ""
    for suffix in (" - LinkedIn", " | Crunchbase"):
        idx = s.find(suffix)
        if idx != -1:
            s = s[:idx].strip()
    return s or (title or "").strip()


def _description_for_organic(o: dict) -> str:
    d = str(o.get("description") or "").strip()
    if d:
        return d
    d = str(o.get("snippet") or "").strip()
    if d:
        return d
    d = str(o.get("displayedUrl") or "").strip()
    if d:
        return d
    return "No description available"


def _organic_to_row(o: dict) -> dict[str, str] | None:
    if not isinstance(o, dict) or not o.get("title"):
        return None
    url = str(o.get("url") or "").strip()
    if not url or "google.com" in url.lower():
        return None
    title = _clean_title(str(o.get("title") or ""))
    if not title:
        return None
    desc = _description_for_organic(o)
    return {"title": title, "url": url, "description": desc}


def _extract_organic_rows(items: list) -> list[dict[str, str]]:
    """
    Each top-level Apify item is either a SERP bundle (organicResults) or a flat organic row.
    Skip google.com / empty URLs. Dedupe by URL, max 10.
    """
    rows: list[dict[str, str]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        if (item.get("type") or "").lower() == "organic" and item.get("title"):
            row = _organic_to_row(item)
            if row:
                rows.append(row)
            continue
        for o in item.get("organicResults") or []:
            row = _organic_to_row(o)
            if row:
                rows.append(row)

    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for r in rows:
        u = r.get("url") or ""
        if u in seen:
            continue
        seen.add(u)
        deduped.append(r)

    out = deduped[:10]
    for r in out:
        print(f"[APIFY] Extracted: {r['title']} — {r['url']}", flush=True)
    return out


async def _emit_leads(obj: dict) -> None:
    await leads_sse_queue.put(obj)


def _drain_leads_queue() -> None:
    while True:
        try:
            leads_sse_queue.get_nowait()
        except asyncio.QueueEmpty:
            break


async def _personalize_row(
    client: AsyncGroq,
    title: str,
    description: str,
) -> dict[str, object]:
    user_prompt = (
        f"Given this company: {title} - {description}. Return ONLY raw JSON with: "
        "company_name (string), pain_point (one sentence about their debt collection or lending qualification pain), "
        "outreach_email (3 sentences: mention their company by name, reference the pain point, pitch Callbook.ai as the AI platform "
        "that helps lenders understand borrowers and recover more), "
        "intent_score (integer 0-100: how strong the fit is for outbound sales / demo booking). "
        "No markdown, raw JSON only."
    )
    try:
        completion = await client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": user_prompt}],
            temperature=0.3,
            response_format={"type": "json_object"},
        )
        raw = (completion.choices[0].message.content or "").strip()
        cleaned = _strip_jsonish(raw)
        parsed = json.loads(cleaned) if cleaned else {}
        if not isinstance(parsed, dict):
            parsed = {}
        raw_score = parsed.get("intent_score", 0)
        try:
            intent_score = int(float(raw_score))
        except (TypeError, ValueError):
            intent_score = 0
        intent_score = max(0, min(100, intent_score))
        return {
            "company_name": str(parsed.get("company_name") or title).strip(),
            "pain_point": str(parsed.get("pain_point") or "").strip(),
            "outreach_email": str(parsed.get("outreach_email") or "").strip(),
            "intent_score": intent_score,
        }
    except Exception:
        return {
            "company_name": title,
            "pain_point": "Unable to infer pain point from search snippet.",
            "outreach_email": "",
            "intent_score": 0,
        }


@router.get("/stream")
async def leads_stream():
    async def event_gen():
        try:
            while True:
                try:
                    msg = await asyncio.wait_for(leads_sse_queue.get(), timeout=15.0)
                except asyncio.TimeoutError:
                    yield {"comment": " ping"}
                    continue
                yield {"data": json.dumps(msg, ensure_ascii=False)}
        except asyncio.CancelledError:
            return

    return EventSourceResponse(
        event_gen(),
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/generate")
async def generate_leads():
    load_dotenv(_ROOT / ".env", override=False)
    _drain_leads_queue()

    token = (os.getenv("APIFY_API_KEY") or "").strip()
    if not token:
        raise HTTPException(status_code=503, detail="APIFY_API_KEY not set in .env")

    groq_key = (os.getenv("GROQ_API_KEY") or "").strip()
    if not groq_key or groq_key == "your_groq_api_key_here":
        raise HTTPException(status_code=503, detail="GROQ_API_KEY not set in .env")

    await _emit_leads(
        {"type": "step", "message": "🔍 Searching Google for lending + debt collection companies..."}
    )
    await _emit_leads({"type": "step", "message": "📡 Apify scraping in progress..."})

    try:
        async with httpx.AsyncClient(timeout=60.0) as http:
            resp = await http.post(
                APIFY_ACTOR_SYNC_URL,
                params={"token": token},
                json=DEFAULT_SCRAPER_INPUT,
            )
    except httpx.TimeoutException as e:
        raise HTTPException(status_code=504, detail="Apify request timed out") from e
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"Apify request failed: {e}") from e

    if resp.status_code >= 400:
        raise HTTPException(
            status_code=resp.status_code if resp.status_code < 500 else 502,
            detail=resp.text[:2000] or "Apify error",
        )

    try:
        items = resp.json()
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=502, detail="Apify returned non-JSON") from e

    if not isinstance(items, list):
        raise HTTPException(status_code=502, detail="Apify returned unexpected payload (expected array)")

    rows = _extract_organic_rows(items)
    if not rows:
        raise HTTPException(status_code=502, detail="No organic search results from Apify")

    for r in rows:
        await _emit_leads({"type": "step", "message": f"✅ Found: {r['title']} — {r['url']}"})

    for r in rows:
        await _emit_leads(
            {"type": "step", "message": f"🧠 Personalizing outreach for {r['title']}..."}
        )

    groq = AsyncGroq(api_key=groq_key)
    tasks = [_personalize_row(groq, r["title"], r["description"]) for r in rows]
    enriched = await asyncio.gather(*tasks)

    out: list[dict] = []
    for r, extra in zip(rows, enriched):
        out.append(
            {
                "title": r["title"],
                "url": r["url"],
                "description": r["description"],
                "company_name": extra["company_name"],
                "pain_point": extra["pain_point"],
                "outreach_email": extra["outreach_email"],
                "intent_score": int(extra.get("intent_score") or 0),
            }
        )

    await _emit_leads({"type": "complete", "leads": out})

    return {"leads": out, "count": len(out)}
