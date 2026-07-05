"""
worker/job_handlers/email_handler.py
Phase 2: Real email via Gmail SMTP (STARTTLS on port 587).
"""

import asyncio
import os
import smtplib
import uuid
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Env vars — defaults sirf host/port ke liye theek hain,
# user/password ka koi default nahi hona chahiye (kyun? socho)
EMAIL_HOST = os.getenv("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "587"))
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")


def _send_smtp(payload: dict) -> dict:
    """
    BLOCKING function — actual SMTP kaam yahan hota hai.
    Isko async handler asyncio.to_thread() se chalayega.
    """
    to = payload["to"]
    subject = payload["subject"]
    body = payload["body"]
    html_body = payload.get("html_body")  # .get() → None if missing, no KeyError

    # ---- 1. Message banao ----
    msg = MIMEMultipart("alternative")  # plain + HTML are alternatives of the same content
    msg["From"] = EMAIL_USER
    msg["To"] = to
    msg["Subject"] = subject

    # Plain text FIRST, HTML second — clients prefer the last attached part
    msg.attach(MIMEText(body, "plain"))
    if html_body:
        msg.attach(MIMEText(html_body, "html"))

    # ---- 2. connect → starttls → login → send ----
    with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT, timeout=30) as server:
        server.starttls()                          # upgrade to encrypted BEFORE any credentials
        server.login(EMAIL_USER, EMAIL_PASSWORD)   # app password, never the main password
        server.send_message(msg)

    # ---- 3. Result return karo (spec ke exact shape mein) ----
    return {
        "sent": True,
        "to": to,
        "message_id": str(uuid.uuid4()),
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "provider": "gmail_smtp",
    }


async def handle_send_email(payload: dict) -> dict:
    """
    Async wrapper — blocking SMTP ko thread mein bhejta hai taaki
    event loop free rahe (same pattern jo CSV/image handlers use karte hain).
    """
    if not EMAIL_USER or not EMAIL_PASSWORD:
        raise ValueError(
            "EMAIL_USER / EMAIL_PASSWORD env vars not set — "
            "add them to .env (use a Gmail App Password, not your main password)"
        )

    print(f"[email_handler] Sending real email to {payload['to']}...")
    result = await asyncio.to_thread(_send_smtp, payload)  # function ref (no parens!) + its arg
    print(f"[email_handler] Email sent to {payload['to']}")
    return result
