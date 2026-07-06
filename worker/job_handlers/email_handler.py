"""
worker/job_handlers/email_handler.py
------------------
PURPOSE: Executes a `send_email` job — sends a REAL email through Gmail's
         SMTP servers (Phase 2 upgrade; Phase 1 was a mock that slept 1s).

HOW IT FITS IN THE SYSTEM:
  Called by worker/worker.py's execute_job() dispatcher whenever a job's
  `type` is "send_email". This is exactly the kind of work a job queue
  exists for: a slow (multi-second), failure-prone external network call
  that must never run inside an HTTP request/response cycle. The API
  answers POST /jobs in milliseconds; THIS code pays the real cost later,
  in the background, with the worker's retry/DLQ machinery around it.

SMTP IN 30 SECONDS:
  SMTP (Simple Mail Transfer Protocol) is the post office of email. Our
  code never contacts the recipient's machine — it hands the message to
  Gmail's SMTP server (smtp.gmail.com:587) and Gmail handles delivery.
  Port 587 uses STARTTLS: the connection starts as plain text, then the
  client sends the STARTTLS command to upgrade to an encrypted (TLS)
  channel BEFORE any credentials are transmitted. Order is critical:
      connect -> starttls -> login -> send
  Logging in before starttls would put the password on the wire in
  plain text, readable by anyone sniffing the network.

AUTHENTICATION:
  EMAIL_PASSWORD must be a Gmail App Password (a 16-character,
  revocable, SMTP-only credential from myaccount.google.com/apppasswords)
  — Google blocks regular account passwords for SMTP entirely, and an App
  Password limits the blast radius if it ever leaks: an attacker could
  send email, but never read the inbox or touch account settings.

IDEMPOTENCY / RETRY-SAFETY:
  Email sending is NOT naturally idempotent — sending twice means the
  recipient gets two emails. Two layers of defense:
    1. worker/worker.py's atomic claim (SELECT FOR UPDATE SKIP LOCKED)
       guarantees only ONE worker ever executes a given job attempt.
    2. Every send stamps a unique message_id into the email's Message-ID
       header AND the stored result — if a duplicate ever slips through
       (e.g. worker crashes AFTER the SMTP send but BEFORE recording
       success, then the retry re-sends), the two copies carry different
       Message-IDs and the stored result identifies which send "won".
       This is at-least-once delivery with detectability — exactly-once
       is impossible over SMTP because Gmail gives us no dedupe hook.
"""

import asyncio
import os
import smtplib
import socket
import uuid
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Read once at import time. Host/port have safe defaults (they're public
# knowledge, identical for every Gmail user); EMAIL_USER/EMAIL_PASSWORD
# deliberately have NO defaults — credentials must come from the
# environment (.env, never committed) or the handler refuses to run.
EMAIL_HOST = os.getenv("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "587"))
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")


def _send_smtp(payload: dict) -> dict:
    """
    BLOCKING function — the actual SMTP conversation happens here.
    Run via asyncio.to_thread() by handle_send_email below, so the
    worker's event loop stays free while this waits on the network.

    ARGS:
      payload: {"to": str, "subject": str, "body": str,
                "html_body": str (optional)} — shape already validated by
                EmailPayload (api/models.py) before the job was queued.

    RETURNS:
      {"sent": True, "to": ..., "message_id": ..., "sent_at": ...,
       "provider": "gmail_smtp"}

    RAISES (all caught by worker.py's retry/DLQ logic):
      ValueError with a human-readable message classifying the failure —
      auth vs recipient vs network — so the error_history in the DLQ
      tells an operator exactly what went wrong without reading tracebacks.
    """
    to = payload["to"]
    subject = payload["subject"]
    body = payload["body"]
    # .get() (not [...]): html_body is optional — returns None when the
    # caller only wants a plain-text email, instead of raising KeyError.
    html_body = payload.get("html_body")

    # Generate the unique ID for THIS send attempt up front, so it can go
    # both into the email's own Message-ID header and the stored result —
    # the pair is what makes duplicate sends detectable (see module
    # docstring, IDEMPOTENCY section).
    message_id = str(uuid.uuid4())

    # ---- 1. Build the message ----
    # "alternative" = the attached parts are ALTERNATIVE renderings of the
    # SAME content (plain text vs HTML). The receiving client picks one.
    # (Contrast with "mixed", which is for attachments — different content
    # pieces meant to be shown together.)
    msg = MIMEMultipart("alternative")
    msg["From"] = EMAIL_USER
    msg["To"] = to
    msg["Subject"] = subject
    # Custom Message-ID ties the delivered email back to our job result.
    # The angle-bracket + domain format is required by RFC 5322.
    msg["Message-ID"] = f"<{message_id}@job-queue-system>"

    # ORDER MATTERS: plain text FIRST, HTML second. Clients prefer the
    # LAST alternative they support — so modern clients render the HTML,
    # and text-only clients fall back to the plain part.
    msg.attach(MIMEText(body, "plain"))
    if html_body:
        msg.attach(MIMEText(html_body, "html"))

    # ---- 2. connect -> starttls -> login -> send ----
    # `with` guarantees the SMTP connection is QUIT + closed even if any
    # step raises — no leaked sockets accumulating across retries.
    # timeout=30: a hung SMTP server fails the job (and triggers a retry)
    # instead of blocking a worker thread forever.
    try:
        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT, timeout=30) as server:
            server.starttls()                         # encrypt BEFORE credentials
            server.login(EMAIL_USER, EMAIL_PASSWORD)  # app password, never main password
            server.send_message(msg)

    except smtplib.SMTPAuthenticationError as exc:
        # Wrong/revoked App Password. Retrying won't help until a human
        # fixes the credential — but letting the normal retry->DLQ flow
        # run is still correct: the job lands in the DLQ with THIS clear
        # message, telling the operator exactly what to fix.
        raise ValueError(
            f"Gmail rejected login for {EMAIL_USER} — check EMAIL_PASSWORD "
            f"is a valid App Password (SMTP code {exc.smtp_code})"
        ) from exc

    except smtplib.SMTPRecipientsRefused as exc:
        # Gmail accepted our login but refused the recipient address
        # (nonexistent mailbox, blocked domain). Permanent for this input.
        raise ValueError(f"Recipient refused by SMTP server: {to} ({exc.recipients})") from exc

    except (smtplib.SMTPException, socket.timeout, OSError) as exc:
        # Everything transient: connection dropped, DNS failure, Gmail
        # temporarily unavailable, network blip. These are the failures
        # exponential backoff + jitter exist for — the retry a few seconds
        # from now genuinely might succeed.
        raise ValueError(f"SMTP send failed (transient?): {type(exc).__name__}: {exc}") from exc

    # ---- 3. Result — the exact shape stored in Postgres's result JSONB ----
    return {
        "sent": True,
        "to": to,
        "message_id": message_id,
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "provider": "gmail_smtp",
    }


async def handle_send_email(payload: dict) -> dict:
    """
    Async wrapper — offloads the blocking SMTP work to a thread so the
    event loop stays free (same pattern the CSV/image handlers use, just
    inverted: they're plain `def` and worker.py wraps them; this one wraps
    itself because it also needs the pre-flight credential check below).
    """
    # Fail fast with an actionable message if credentials never made it
    # into the container (missing .env entry, typo'd var name) — far
    # better than smtplib's generic "authentication failed" later.
    if not EMAIL_USER or not EMAIL_PASSWORD:
        raise ValueError(
            "EMAIL_USER / EMAIL_PASSWORD env vars not set — "
            "add them to .env (use a Gmail App Password, not your main password)"
        )

    print(f"[email_handler] Sending real email to {payload['to']}...")
    # Function REFERENCE (no parens!) + its argument. Writing
    # _send_smtp(payload) would call it immediately on the event loop
    # thread — blocking everything — and hand to_thread the resulting
    # dict instead of a callable.
    result = await asyncio.to_thread(_send_smtp, payload)
    print(f"[email_handler] Email sent to {payload['to']}")
    return result
