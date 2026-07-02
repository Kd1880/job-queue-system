"""
worker/job_handlers/__init__.py
------------------
PURPOSE: Central dispatch table mapping a job's `type` string to the
         function that actually executes it.

HOW IT FITS IN THE SYSTEM:
  worker/worker.py's execute_job() looks up job_data["type"] in
  JOB_HANDLERS to find the right function, instead of an if/elif chain.
  This is the ONE place that needs editing to add a new job type in the
  future: write a new handler module, import it here, add one dict entry —
  api/models.py's JobType enum is the only other place a new type must be
  registered (so the API accepts and validates it).
"""

from worker.job_handlers.csv_handler import handle_process_csv
from worker.job_handlers.email_handler import handle_send_email
from worker.job_handlers.image_handler import handle_resize_image

# Maps job type string -> handler function. handle_send_email is `async
# def` (I/O-bound, simulates waiting on a network call); the other two are
# plain `def` (CPU/disk-bound). worker/worker.py's execute_job() checks
# which kind it got via asyncio.iscoroutinefunction() and dispatches
# accordingly (awaiting directly vs. running in a thread).
JOB_HANDLERS = {
    "send_email": handle_send_email,
    "process_csv": handle_process_csv,
    "resize_image": handle_resize_image,
}
