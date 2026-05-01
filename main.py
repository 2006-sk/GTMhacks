import asyncio
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from groq import AsyncGroq

from routers import agent, graphql_api, redis_state, sse, vapi, whisper
from routers.agent import LEAD_HASH_KEY, _lead_to_redis_mapping, _redis_hash_to_lead
from routers.redis_state import sse_queue

load_dotenv(Path(__file__).resolve().parent / ".env")


@asynccontextmanager
async def lifespan(app: FastAPI):
    redis_state.init_redis_pool()
    state_task = asyncio.create_task(redis_state.state_updater_worker())
    sse_task = asyncio.create_task(redis_state.sse_pusher_worker())
    agent_task = asyncio.create_task(agent.agent_worker())
    yield
    state_task.cancel()
    sse_task.cancel()
    agent_task.cancel()
    await asyncio.gather(state_task, sse_task, agent_task, return_exceptions=True)
    redis_state.close_redis_pool()


app = FastAPI(title="MeetingMind OS", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(vapi.router, prefix="/vapi", tags=["vapi"])
app.include_router(agent.router, prefix="/api/agent", tags=["agent"])
app.include_router(redis_state.router, prefix="/api/redis", tags=["redis"])
app.include_router(sse.router, prefix="/sse", tags=["sse"])
app.include_router(graphql_api.router, prefix="/graphql", tags=["graphql"])
app.include_router(whisper.router, prefix="/whisper", tags=["whisper"])


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/lead/status")
async def lead_status():
    r = redis_state.get_redis()
    h = await asyncio.to_thread(r.hgetall, LEAD_HASH_KEY)
    return _redis_hash_to_lead(h or {})


@app.post("/call/start")
async def call_start():
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
    return {"status": "call started"}


@app.post("/call/reset")
async def call_reset():
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
    return {"status": "reset"}


@app.post("/call/end")
async def call_end():
    r = redis_state.get_redis()

    # Mark call outcome flags
    existing = _redis_hash_to_lead(await asyncio.to_thread(r.hgetall, LEAD_HASH_KEY) or {})
    updated = {**existing, "demo_booked": True, "followup_sent": True}
    await asyncio.to_thread(r.hset, LEAD_HASH_KEY, mapping=_lead_to_redis_mapping(updated))

    groq_key = (os.getenv("GROQ_API_KEY") or "").strip()
    if not groq_key or groq_key == "your_groq_api_key_here":
        email = ""
    else:
        client = AsyncGroq(api_key=groq_key)
        prompt = (
            "Write a concise follow-up email for this fintech lead in exactly 3 sentences. "
            "Be specific, friendly, and include a clear CTA to book a demo. "
            "Return plain text only."
        )
        completion = await client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": json.dumps(_redis_hash_to_lead(_lead_to_redis_mapping(updated)), ensure_ascii=False)},
            ],
            temperature=0.2,
        )
        email = (completion.choices[0].message.content or "").strip()

    await asyncio.to_thread(r.set, "followup:email", email)
    await sse_queue.put({"type": "call_ended", "email": email})
    return {"email": email}


# Mount the frontend last so API routes take priority.
app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")
