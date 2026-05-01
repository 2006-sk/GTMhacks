from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from fastapi import APIRouter, File, HTTPException, UploadFile
from groq import AsyncGroq

router = APIRouter()

_ROOT = Path(__file__).resolve().parent.parent


@router.post("/transcribe")
async def whisper_transcribe(audio: UploadFile = File(...)):
    """
    Transcribe an uploaded audio file using Groq Whisper.
    Expects multipart/form-data with field name "audio".
    """
    load_dotenv(_ROOT / ".env", override=False)
    api_key = (os.getenv("GROQ_API_KEY") or "").strip()
    if not api_key or api_key == "your_groq_api_key_here":
        raise HTTPException(status_code=503, detail="GROQ_API_KEY not set")

    audio_bytes = await audio.read()
    if not audio_bytes:
        raise HTTPException(status_code=400, detail="Empty audio upload")

    client = AsyncGroq(api_key=api_key)
    try:
        resp = await client.audio.transcriptions.create(
            model="whisper-large-v3-turbo",
            file=(audio.filename or "audio", audio_bytes, audio.content_type or "application/octet-stream"),
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e

    text = (getattr(resp, "text", None) or "").strip()
    return {"text": text}

