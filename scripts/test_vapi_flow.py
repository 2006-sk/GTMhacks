#!/usr/bin/env python3
"""
Exercise the backend Vapi pipeline without the browser:
  POST /vapi/webhook (call-started + assistant + user lines)
  → conversation_state + Redis transcript stream (if configured)
  → agent_worker → Groq → Redis lead hash + sse_queue
  GET /lead/status for resulting lead
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")


def main() -> int:
    from fastapi.testclient import TestClient

    from main import app

    with TestClient(app) as client:
        h = client.get("/api/health")
        if h.status_code != 200:
            print("FAIL health:", h.status_code, h.text)
            return 1
        print("ok GET /api/health")

        client.post("/call/reset")

        r = client.post("/vapi/webhook", json={"type": "call-started"})
        if r.status_code != 200 or r.json().get("status") != "ok":
            print("FAIL call-started webhook:", r.status_code, r.text)
            return 1
        print("ok POST /vapi/webhook call-started")

        intro = (
            "Hi. This is Alex from Callbook.ai. I'm reaching out because we help teams turn "
            "large outbound call lists into live conversations with CRM-synced outcomes. "
            "Do you have two minutes to chat?"
        )
        r = client.post("/vapi/webhook", json={"speaker": "assistant", "text": intro})
        if r.status_code != 200:
            print("FAIL assistant webhook:", r.status_code, r.text)
            return 1
        print("ok POST /vapi/webhook assistant transcript (browser-style payload)")

        time.sleep(2.5)
        lead = client.get("/lead/status")
        if lead.status_code != 200:
            print("FAIL /lead/status (Redis may be unset in .env):", lead.status_code, lead.text)
            return 1
        body = lead.json()
        print(
            "ok GET /lead/status after assistant:",
            f"score={body.get('lead_score')} intent={body.get('intent')}",
        )

        user_line = (
            "We're a mid-size lender with maybe twenty thousand stale contacts and a manual dialer; "
            "follow-up is slow and we're hiring more collectors."
        )
        r = client.post("/vapi/webhook", json={"speaker": "user", "text": user_line})
        if r.status_code != 200:
            print("FAIL user webhook:", r.status_code, r.text)
            return 1
        print("ok POST /vapi/webhook user transcript")

        time.sleep(4.0)
        lead2 = client.get("/lead/status").json()
        print(
            "ok GET /lead/status after user:",
            f"score={lead2.get('lead_score')} intent={lead2.get('intent')} stage={lead2.get('pipeline_stage')}",
        )

        vt = client.get("/vapi/test")
        if vt.status_code == 200:
            print("ok GET /vapi/test", vt.json())

        st = client.get("/sse/test")
        if st.status_code != 200:
            print("WARN /sse/test:", st.status_code)
        else:
            print("ok GET /sse/test (enqueued ping on sse_queue)")

    print("\nAll backend steps completed. With Redis + GROQ_API_KEY, scores should move after user line.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
