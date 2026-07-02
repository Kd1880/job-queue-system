"""
worker/worker.py
------------------
PURPOSE: The main worker process. Runs forever in its own container,
         pulling jobs off the Redis queue one at a time, executing them,
         and recording the outcome in Postgres.

HOW IT FITS IN THE SYSTEM:
  api/routes/jobs.py LPUSHes a job JSON string onto the Redis list
  "jobs:queue" the instant it's submitted. THIS file is the other end of
  that handoff: it BRPOPs from the same list, executes the job via the
  appropriate handler in worker/job_handlers/, and writes the result back
  to the SAME Postgres row the API created — closing the loop:
      pending (api) -> running (here) -> completed | failed (here)

WHY THIS IS A SEPARATE PROCESS/CONTAINER FROM THE API:
  The API must respond to HTTP requests in milliseconds; job execution can
  take anywhere from 1 second (mock email) to many seconds (large CSV,
  many image sizes). Running them in the same process would mean a slow
  job blocks the API from answering other users. Splitting them lets each
  scale and fail independently — a crashed worker never takes down the
  HTTP server, and a burst of slow jobs never makes POST /jobs slow to
  respond.

THE JOB LIFECYCLE THIS FILE IMPLEMENTS, END TO END:
  1. BRPOP blocks until a job appears on the queue (or times out, to allow
     periodic heartbeat/shutdown checks).
  2. IDEMPOTENCY CHECK: has this exact job_id already been completed or is
     it already running? If so, skip — never execute the same job twice.
  3. Mark the job 'running' in Postgres (started_at = now).
  4. Execute it via the type-specific handler in worker/job_handlers/.
  5. On success: mark 'completed', store the result.
  6. On failure: if retries remain, wait with exponential backoff and
     re-queue; if retries are exhausted, move the job to the
     dead_letter_queue table and mark it permanently 'failed'.
  7. Never crash: every exception is caught so ONE broken job can never
     take down the whole worker process — the loop always continues to
     the next job.
"""

import asyncio
import json
import random

from worker.job_handlers import JOB_HANDLERS
from worker.utils import (
    MAX_RETRIES,
    QUEUE_KEY,
    WORKER_TIMEOUT,
    create_db_pool,
    create_redis_client,
    get_job,
    log_event,
    move_to_dlq,
    requeue_job,
    update_job_completed,
    update_job_failed,
    update_job_retry,
    update_job_running,
)


async def execute_job(job_data: dict):
    """
    Dispatch a job to its type-specific handler and return the result.

    ARGS:
      job_data: the full job dict popped from Redis, e.g.
                {"id": "...", "type": "process_csv", "payload": {...},
                 "retry_count": 0}

    WHY THE THREAD-VS-COROUTINE CHECK:
      handle_send_email is `async def` — it awaits asyncio.sleep(1), which
      is non-blocking and yields control back to the event loop.
      handle_process_csv / handle_resize_image are plain `def` — pandas
      and Pillow have no async API and do real synchronous CPU/disk work.
      Calling a blocking function directly inside an async function would
      freeze the ENTIRE event loop for however long that job takes.
      asyncio.to_thread() runs it in a separate worker thread instead, so
      the event loop stays free (important once Phase 2 adds concurrent
      job handling within one worker process).
    """
    # Look up the type-specific handler function, e.g. "process_csv" ->
    # handle_process_csv. A KeyError here would mean a job type slipped
    # past API validation somehow — an unexpected situation the caller's
    # try/except in process_job() below will catch and route to retry/DLQ.
    handler = JOB_HANDLERS[job_data["type"]]

    if asyncio.iscoroutinefunction(handler):
        # async handler (currently only send_email) — await it directly,
        # it already yields control cooperatively via asyncio.sleep.
        return await handler(job_data["payload"])
    else:
        # sync handler (process_csv, resize_image) — offload to a thread
        # so blocking pandas/Pillow work doesn't stall the event loop.
        return await asyncio.to_thread(handler, job_data["payload"])


async def process_job(job_data: dict, pool, redis_client) -> None:
    """
    Run one job through its full lifecycle: idempotency check -> mark
    running -> execute -> mark completed/retry/failed.

    This function is intentionally allowed to raise: worker.py's main loop
    wraps EVERY call to it in a try/except so that even a bug in this
    lifecycle logic itself (not just in a job handler) can never crash the
    whole worker process.
    """
    # job_id uniquely identifies this job across Redis and Postgres — the
    # same UUID the API generated and stored as the Postgres primary key.
    job_id = job_data["id"]

    # ------------------------------------------------------------------
    # STEP 1: IDEMPOTENCY CHECK
    # Fetch the job's current status from Postgres BEFORE doing any work.
    # WHY: it's possible for a job_id to be seen twice by workers — e.g. a
    # job was manually re-pushed to Redis, or (in Phase 2, with multiple
    # worker replicas) two workers could theoretically pop the same
    # message in a narrow race window. If the job is already 'completed'
    # or already 'running' (started by another worker), skip it entirely
    # rather than risk double-sending an email or double-processing a
    # file — side-effecting operations must never run twice.
    # ------------------------------------------------------------------
    existing = await get_job(pool, job_id)
    if existing is not None and existing["status"] in ("completed", "running"):
        log_event("SKIPPED", job_id, extra=f"already {existing['status']}")
        return

    # ------------------------------------------------------------------
    # STEP 2: MARK RUNNING
    # Write status='running' + started_at=now() to Postgres BEFORE
    # executing. This is the checkpoint that makes "started but crashed"
    # distinguishable from "never started" if the worker process dies
    # mid-job (see worker/utils.py::update_job_running for more detail).
    # ------------------------------------------------------------------
    await update_job_running(pool, job_id)
    log_event("STARTED", job_id, extra=f"type={job_data['type']}")

    # ------------------------------------------------------------------
    # STEP 3: EXECUTE, THEN HANDLE SUCCESS OR FAILURE
    # ------------------------------------------------------------------
    try:
        # Run the actual job logic (mock email send, real CSV cleaning,
        # real image resizing) via the type-specific handler.
        result = await execute_job(job_data)

        # SUCCESS: persist the result and flip status to 'completed'.
        # This is the terminal happy-path state — the worker will never
        # touch this job_id again.
        await update_job_completed(pool, job_id, result)
        log_event("COMPLETED", job_id, extra=f"result={result}")

    except Exception as exc:
        # FAILURE: any exception from ANY handler lands here — a missing
        # file, a malformed CSV, a Pillow decode error, or anything else.
        # We deliberately catch the broadest Exception (not a specific
        # type) because job handlers are user-supplied-ish code (new job
        # types will be added later) and we can never predict every way
        # they might fail — the one guarantee we need is that failure
        # here NEVER escapes to crash worker.py's main loop.
        error_message = str(exc)

        # How many times has this specific job already been retried?
        # Defaults to 0 for a job's first-ever attempt.
        retry_count = job_data.get("retry_count", 0)

        # Running list of every error message across all attempts so far
        # (not just this one) — carried inside job_data itself as it
        # travels back through Redis, so that if this job eventually
        # exhausts its retries, move_to_dlq() below has the FULL failure
        # history to store, not just the final error.
        error_history = job_data.get("error_history", [])
        error_history.append(error_message)

        if retry_count < MAX_RETRIES:
            # --------------------------------------------------------
            # RETRY PATH: exponential backoff, then re-queue.
            # --------------------------------------------------------
            # Exponential backoff: 2^0=1s, 2^1=2s, 2^2=4s (+ up to 1s of
            # random jitter each time). WHY exponential (not a fixed
            # delay): if the failure is caused by a temporarily
            # overloaded downstream system, retrying immediately would
            # just hammer it again; backing off gives it time to recover,
            # and growing the delay on each successive failure avoids
            # piling on harder the more clearly something is actually
            # broken. WHY random jitter: if many jobs failed at the same
            # moment (e.g. Redis blipped), pure exponential backoff would
            # have them ALL retry at the exact same instant again —
            # jitter spreads retries out over time.
            wait_seconds = (2 ** retry_count) + random.uniform(0, 1)
            log_event(
                "RETRY_SCHEDULED",
                job_id,
                extra=f"attempt={retry_count + 1}/{MAX_RETRIES} wait={wait_seconds:.2f}s error={error_message}",
            )

            # Block here (this coroutine only — asyncio.sleep yields the
            # event loop, though Phase 1 has nothing else running
            # concurrently to yield to) before re-queuing, so the failed
            # job doesn't immediately retry in a tight, wasteful loop.
            await asyncio.sleep(wait_seconds)

            # Mutate the job dict that will be re-pushed to Redis: bump
            # the retry counter and carry the accumulated error history
            # forward, so the NEXT attempt (or eventual DLQ entry) knows
            # exactly how many times this job has failed and why.
            job_data["retry_count"] = retry_count + 1
            job_data["error_history"] = error_history

            # LPUSH the job back onto the SAME queue new jobs use — see
            # worker/utils.py::requeue_job for why this keeps retried jobs
            # in simple FIFO order with everything else.
            await requeue_job(redis_client, job_data)

            # Record the retry in Postgres too: status goes back to
            # 'pending' (it's queued again, not done), retry_count is
            # updated, and error_message shows the reason for this most
            # recent failure — visible to anyone polling GET /jobs/{id}.
            await update_job_retry(pool, job_id, retry_count + 1, error_message)

        else:
            # --------------------------------------------------------
            # DEAD LETTER PATH: MAX_RETRIES exhausted, give up.
            # --------------------------------------------------------
            # Insert a permanent record into dead_letter_queue with the
            # FULL error_history (every attempt's failure reason, not
            # just the last) so a human reviewing the DLQ later has the
            # complete picture — see worker/utils.py::move_to_dlq.
            await move_to_dlq(pool, job_id, job_data["type"], job_data["payload"], error_history)

            # Mark the job's own row as permanently 'failed'. This IS a
            # terminal state — unlike the retry path's 'pending', no
            # future worker iteration will ever pick this job_id up
            # again unless a human manually re-submits it.
            await update_job_failed(pool, job_id, error_message)

            log_event(
                "MOVED_TO_DLQ",
                job_id,
                extra=f"attempts={retry_count + 1} final_error={error_message}",
            )


async def main() -> None:
    """
    The worker's entry point: set up connections, then loop forever
    pulling and processing jobs.
    """
    log_event("WORKER_STARTING", "-")

    # Open the worker's own Postgres connection pool and Redis client —
    # see worker/utils.py for why these are separate from the API's
    # (different container, different Docker build context, intentionally
    # decoupled services).
    pool = await create_db_pool()
    redis_client = create_redis_client()

    log_event("WORKER_READY", "-", extra=f"queue={QUEUE_KEY} max_retries={MAX_RETRIES} poll_timeout={WORKER_TIMEOUT}s")

    try:
        # The main loop: runs for the entire lifetime of the container.
        # docker-compose.yml sets `restart: unless-stopped` on the worker
        # service, so even if this loop somehow exits via an unhandled
        # crash, Docker brings the whole process back up automatically —
        # a second line of defense behind the try/except inside the loop
        # itself.
        while True:
            # ----------------------------------------------------------
            # BRPOP = Blocking Right Pop.
            # 'Blocking' means: if "jobs:queue" is empty, this call
            # SLEEPS here inside Redis itself — it does NOT spin in a
            # busy loop repeatedly checking an empty list, which would
            # waste CPU for nothing. This is far more efficient than
            # polling with a plain RPOP + manual sleep.
            # timeout=WORKER_TIMEOUT (seconds): wake up periodically even
            # when no job arrives, so this loop gets a chance to run
            # again — in Phase 1 that just means going right back to
            # BRPOP, but this is also where a future graceful-shutdown
            # signal check would go, and it keeps the process from
            # blocking forever in a way that looks "hung" from the
            # outside.
            # RPOP (right end) is paired with the API's LPUSH (left end)
            # — see worker/utils.py::requeue_job and
            # api/redis_client.py::push_job — giving strict FIFO order:
            # first job submitted is the first job a worker receives.
            # ----------------------------------------------------------
            result = await redis_client.brpop(QUEUE_KEY, timeout=WORKER_TIMEOUT)

            if result is None:
                # Timed out with no job available — loop back to BRPOP
                # and keep waiting. This is the normal "queue is empty"
                # state, not an error.
                continue

            # BRPOP returns a (key, value) tuple when it succeeds; we only
            # need the value (the job JSON string) — the key is always
            # QUEUE_KEY since that's the only list we're watching.
            _, job_json = result

            # Parse the job JSON. WHY a dedicated try/except just for
            # this: if the payload isn't valid JSON at all (which should
            # never happen given only api/routes/jobs.py ever writes to
            # this queue, but defensive parsing costs nothing), we can't
            # even extract a job_id to log against — so we log the raw
            # parse failure and drop the message rather than let a
            # malformed message repeatedly crash the loop.
            try:
                job_data = json.loads(job_json)
            except json.JSONDecodeError as exc:
                log_event("MALFORMED_JOB_DROPPED", "-", extra=f"error={exc} raw={job_json!r}")
                continue

            job_id = job_data.get("id", "unknown")
            log_event("PICKED_UP", job_id, extra=f"type={job_data.get('type')}")

            # ----------------------------------------------------------
            # THE CORE "NEVER CRASH THE WORKER" GUARANTEE:
            # process_job() handles its OWN internal errors (job handler
            # exceptions -> retry or DLQ, see above). This outer
            # try/except is a final safety net for anything else that
            # could go wrong around it — e.g. a transient Postgres
            # connection drop while writing the 'running' status update.
            # Whatever happens, we log it and continue to the next
            # iteration; we never let one bad job take down the process
            # that's supposed to keep processing every job after it.
            # ----------------------------------------------------------
            try:
                await process_job(job_data, pool, redis_client)
            except Exception as exc:
                log_event("WORKER_LOOP_ERROR", job_id, extra=f"error={exc}")
    finally:
        # Only reached if the `while True` loop somehow exits (it
        # shouldn't, in normal operation) — close connections cleanly so
        # Postgres/Redis see a graceful disconnect.
        await pool.close()
        await redis_client.aclose()


if __name__ == "__main__":
    # asyncio.run() creates the event loop, runs main() to completion (in
    # practice: forever, until the container is stopped), and tears the
    # loop down cleanly on exit/interrupt.
    asyncio.run(main())
