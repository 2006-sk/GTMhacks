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
                        "You are Alex, a consultative outbound GTM representative for Callbook.ai. "
                        "Your goal is to qualify prospects in under 3 minutes for high-volume outbound "
                        "calling workflows where missed contacts, slow follow-up, or manual call-center "
                        "labor create measurable revenue leakage. Ask about their organization type, "
                        "call volume, past-due or stale contact lists, CRM or dialer stack, follow-up "
                        "speed, compliance requirements, and timeline to improve the workflow. Prioritize "
                        "collections-heavy and CRM-driven teams such as lenders, fintechs, loan servicers, "
                        "healthcare revenue-cycle teams, education providers, insurance companies, and "
                        "appointment-based businesses. Be conversational, confident, concise, and ask one "
                        "question at a time. Do not claim guaranteed compliance or guaranteed revenue "
                        "outcomes. Never say you are an AI."
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
