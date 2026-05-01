#!/usr/bin/env python3
"""Patch Vapi assistant: webhook URL, first message, and Callbook.ai system prompt (via .env)."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import httpx
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent


def main() -> None:
    load_dotenv(ROOT / ".env")

    private_key = os.getenv("VAPI_PRIVATE_KEY", "").strip()
    assistant_id = os.getenv("VAPI_ASSISTANT_ID", "").strip()
    ngrok_url = os.getenv("NGROK_URL", "").strip()

    missing = [
        name
        for name, val in (
            ("VAPI_PRIVATE_KEY", private_key),
            ("VAPI_ASSISTANT_ID", assistant_id),
            ("NGROK_URL", ngrok_url),
        )
        if not val
    ]
    if missing:
        print("Missing or empty in .env:", ", ".join(missing), file=sys.stderr)
        sys.exit(1)

    url = f"https://api.vapi.ai/assistant/{assistant_id}"
    headers = {
        "Authorization": f"Bearer {private_key}",
        "Content-Type": "application/json",
    }

    server_url = f"{ngrok_url.rstrip('/')}/vapi/webhook"
    body = {
        "server": {"url": server_url},
        # Shorter delays so the assistant feels responsive after the user speaks.
        "responseDelaySeconds": 1,
        "llmRequestDelaySeconds": 0,
        # Let the user interrupt the intro quickly; helps when it feels "stuck" talking.
        "stopSpeakingPlan": {
            "numWords": 0,
            "voiceSeconds": 0.2,
            "backoffSeconds": 0.5,
        },
        "startSpeakingPlan": {
            "waitSeconds": 0.4,
            "smartEndpointingPlan": {"provider": "vapi"},
        },
        "firstMessage": (
            "Hi, this is Alex from Callbook.ai. I'm reaching out because we help teams turn "
            "large outbound call lists into live conversations with CRM-synced outcomes. "
            "Do you have 2 minutes to chat?"
        ),
        "model": {
            "provider": "openai",
            "model": "gpt-4o-mini",
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are Alex, a fintech sales development rep for FinTrust. Your goal is to "
                        "qualify leads in under 3 minutes. Ask about: their company size, current pain "
                        "points with lead qualification or collections, budget range, compliance "
                        "requirements (SOC2, CFPB), and timeline to purchase. Be conversational, "
                        "confident, and concise. One question at a time. Never say you are an AI."
                    ),
                }
            ],
        },
    }

    try:
        with httpx.Client(timeout=60.0) as client:
            response = client.patch(url, headers=headers, json=body)
    except Exception as e:
        print("Request failed:", str(e), file=sys.stderr)
        sys.exit(2)

    print("status:", response.status_code)
    try:
        print(json.dumps(response.json(), indent=2))
    except (ValueError, json.JSONDecodeError):
        print(response.text)


if __name__ == "__main__":
    main()
