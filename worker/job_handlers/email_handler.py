"""
worker/job_handlers/email_handler.py
------------------
PURPOSE: Executes a `send_email` job.

HOW IT FITS IN THE SYSTEM:
  Called by worker/worker.py's execute_job() dispatcher whenever a job's
  `type` is "send_email". Phase 1 has no real email provider wired up
  (no SendGrid/SES credentials, no SMTP server) — this is a MOCK
  implementation that simulates the latency and output shape of a real
  email API, so the rest of the pipeline (retry logic, status transitions,
  result storage) can be built and tested end-to-end without needing real
  external infrastructure. Swapping this mock for a real provider later is
  a one-function change — nothing else in the system needs to know.
"""

import asyncio
import uuid
from datetime import datetime, timezone


async def handle_send_email(payload: dict) -> dict:
    """
    Simulate sending an email.

    ARGS:
      payload: {"to": str, "subject": str, "body": str} — already
               validated against EmailPayload by the API layer
               (api/models.py) before this job ever reached the queue, so
               we trust the shape here without re-validating.

    FLOW:
      1. Log the "send" so worker output shows exactly what would have
         gone out.
      2. Sleep 1 second — simulates the network round-trip latency of a
         real email provider's API call. This is what makes send_email a
         useful test case for the queue: it's an example of a job whose
         execution time is dominated by waiting on an external service,
         not local CPU work (unlike process_csv/resize_image).
      3. Return a result shaped like what a real provider would give back
         (a message_id you could use to track delivery status).

    RETURNS:
      {"message_id": "mock-<uuid>", "sent_at": "<iso timestamp>", "to": "<email>"}
    """
    to = payload["to"]

    # This function is `async def` (not a plain function run via
    # asyncio.to_thread like the CSV/image handlers) specifically because
    # asyncio.sleep is non-blocking — it yields control back to the event
    # loop instead of holding a thread hostage for a full second, which
    # matters once Phase 2 runs multiple jobs concurrently in one worker
    # process.
    print(f"[email_handler] Sending email to {to}...")
    await asyncio.sleep(1)
    print(f"[email_handler] Email sent to {to}")

    return {
        "message_id": f"mock-{uuid.uuid4()}",
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "to": to,
    }
