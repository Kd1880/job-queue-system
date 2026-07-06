
import json
import os
from datetime import datetime, timezone
from typing import Any, Optional

import asyncpg
import redis.asyncio as redis


DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/jobqueue"
)
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")

QUEUE_KEY = "jobs:queue"

PROCESSING_SET_KEY = "jobs:processing"

# Read from the environment so `docker-compose.yml`'s .env is the single
# place retry behavior is tuned, without editing code.
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
WORKER_TIMEOUT = int(os.environ.get("WORKER_TIMEOUT", "5"))


async def create_db_pool() -> asyncpg.Pool:
   
    return await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)


def create_redis_client() -> redis.Redis:
   
    return redis.from_url(REDIS_URL, decode_responses=True)


def log_event(event: str, job_id: str, extra: str = "") -> None:

    worker_id = os.environ.get("WORKER_ID", "worker-main")
    timestamp = datetime.now(timezone.utc).isoformat()
    suffix = f" {extra}" if extra else ""
    print(f"[{timestamp}] [{worker_id}] {event} job_id={job_id}{suffix}", flush=True)


async def get_job(pool: asyncpg.Pool, job_id: str) -> Optional[asyncpg.Record]:
    """
    Fetch a job's current status by primary key.

    WHY THE WORKER NEEDS THIS (IDEMPOTENCY): between a job being LPUSHed
    and BRPOPed, it's possible (in Phase 2, with multiple workers; even in
    Phase 1, if a job was manually re-pushed) for the SAME job_id to be
    read from the queue more than once. Before doing any work, the worker
    checks: has this job already been completed, or is it already running
    (picked up by a concurrent worker)? If so, skip it — this makes job
    execution idempotent instead of risking double-sending an email or
    double-processing a CSV.

    Primary-key lookup — O(1) via Postgres's automatic PK index.
    """
    return await pool.fetchrow(
        "SELECT id, status, retry_count FROM jobs WHERE id = $1",
        job_id,
    )


async def update_job_running(pool: asyncpg.Pool, job_id: str) -> None:

    await pool.execute(
        "UPDATE jobs SET status = 'running', started_at = $2 WHERE id = $1",
        job_id,
        datetime.now(timezone.utc).replace(tzinfo=None),
    )


async def update_job_completed(pool: asyncpg.Pool, job_id: str, result: dict) -> None:
    """
    Mark a job as 'completed', store its result, and stamp completed_at.

    result is stored as JSONB (handler-specific shape — e.g. CSV stats or
    a list of resized image paths). Clearing error_message back to NULL
    handles the case where this job previously failed a retry attempt
    (error_message was set) and then succeeded on a later attempt — we
    don't want a stale error message sitting on a job that ultimately
    succeeded.
    """
    await pool.execute(
        """
        UPDATE jobs
        SET status = 'completed',
            result = $2::jsonb,
            error_message = NULL,
            completed_at = $3
        WHERE id = $1
        """,
        job_id,
        json.dumps(result),
        datetime.now(timezone.utc).replace(tzinfo=None),
    )


async def update_job_retry(pool: asyncpg.Pool, job_id: str, retry_count: int, error_message: str) -> None:
    """
    Record a failed attempt that's about to be retried.

    Status goes back to 'pending' (not 'failed') because the job is being
    re-queued, not given up on — 'failed' is reserved for the terminal
    state after MAX_RETRIES is exhausted (see update_job_failed below).
    error_message is updated to the LATEST failure reason so anyone
    polling GET /jobs/{id} mid-retry can see why the most recent attempt
    failed, even though the job is still technically 'pending' another try.
    """
    await pool.execute(
        """
        UPDATE jobs
        SET status = 'pending',
            retry_count = $2,
            error_message = $3
        WHERE id = $1
        """,
        job_id,
        retry_count,
        error_message,
    )


async def update_job_failed(pool: asyncpg.Pool, job_id: str, error_message: str) -> None:
    """
    Mark a job as permanently 'failed' — MAX_RETRIES has been exhausted.
    This is a terminal state: the worker will never touch this job_id
    again unless a human manually re-submits it. completed_at is still
    stamped (even though the job didn't succeed) so callers can measure
    "how long did we spend before giving up" the same way they'd measure
    successful job duration.
    """
    await pool.execute(
        """
        UPDATE jobs
        SET status = 'failed',
            error_message = $2,
            completed_at = $3
        WHERE id = $1
        """,
        job_id,
        error_message,
        datetime.now(timezone.utc).replace(tzinfo=None),
    )


async def move_to_dlq(
    pool: asyncpg.Pool,
    job_id: str,
    job_type: str,
    payload: dict,
    error_log: list[str],
) -> None:
    """
    Insert a permanent record into dead_letter_queue for a job that
    exhausted all retries.

    WHY A SEPARATE TABLE (instead of just leaving status='failed' on the
    jobs row): the DLQ is meant for human triage — an operator scanning
    "what's broken and needs manual attention" shouldn't have to filter the
    entire jobs table by status. error_log captures the FULL history of
    every failure across all retry attempts (not just the final one),
    which is often much more diagnostic than a single error message alone
    (e.g. seeing the same "connection refused" three times in a row points
    at infrastructure, not the job's input data).
    """
    await pool.execute(
        """
        INSERT INTO dead_letter_queue
            (original_job_id, job_type, original_payload, error_log)
        VALUES ($1, $2, $3::jsonb, $4)
        """,
        job_id,
        job_type,
        json.dumps(payload),
        error_log,
    )


async def requeue_job(redis_client: redis.Redis, job_data: dict) -> None:
    """
    Push a job that failed (but has retries remaining) back onto the
    queue for another attempt.

    LPUSH = Left Push, same operation the API uses for brand-new job
    submissions (see api/redis_client.py::push_job). Using the same
    LPUSH/BRPOP pair means retried jobs re-enter the exact same FIFO queue
    as new jobs — they don't jump ahead of jobs submitted before them,
    keeping worker behavior simple and predictable (no separate
    "priority" or "retry" queue to reason about in Phase 1).
    """
    await redis_client.lpush(QUEUE_KEY, json.dumps(job_data))


# ============================================================================
# PHASE 2: RELIABILITY PRIMITIVES
# ============================================================================

async def claim_job(pool: asyncpg.Pool, job_id: str) -> bool:
    """
    Atomically claim a job for THIS worker. Returns True if we won the
    claim (and the job is now 'running'), False if someone else did (or
    the job is already completed/failed) — in which case the caller must
    skip it entirely.

    THE RACE CONDITION THIS FIXES:
      Phase 1's check was two separate steps:
          1. SELECT status FROM jobs WHERE id = X   -- reads 'pending'
          2. UPDATE jobs SET status = 'running'...
      With 3 workers, two of them can BOTH run step 1 in the same
      millisecond, BOTH see 'pending', and BOTH proceed to execute the
      job — the recipient gets two emails. The read and the write must be
      ONE indivisible operation, which is exactly what a transaction +
      row lock provides.

    HOW EACH PIECE CONTRIBUTES:
      conn.transaction()  — BEGIN/COMMIT. The row lock acquired by the
        SELECT below lives until COMMIT, so the status check and the
        UPDATE happen under one continuous lock: no other worker can
        touch the row between our read and our write.
      FOR UPDATE — locks the selected row. Any other transaction trying
        to lock the same row must WAIT until we commit; by then status is
        'running' and their WHERE status='pending' no longer matches.
      SKIP LOCKED — the crucial extra: instead of WAITING for a locked
        row (a queue of workers stacking up behind one job, all but one
        discovering it's taken), a competing worker gets zero rows back
        IMMEDIATELY and moves on to its next BRPOP. Losing a claim race
        costs microseconds instead of a blocked connection. This
        SELECT ... FOR UPDATE SKIP LOCKED pattern is THE standard way to
        build a job queue on Postgres.

    WHY status = 'pending' IN THE WHERE CLAUSE: it makes the claim doubly
    safe — a job that's already 'running' (claimed by a live worker),
    'completed', or 'failed' simply doesn't match, so duplicate deliveries
    of the same job_id through Redis become harmless no-ops. This replaces
    Phase 1's separate read-then-check idempotency logic with one atomic
    operation.
    """
    # pool.acquire(): transactions need ALL their statements on the SAME
    # connection (a pool hands different statements to different
    # connections otherwise, and BEGIN on one connection does nothing for
    # a statement running on another).
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                SELECT id FROM jobs
                WHERE id = $1 AND status = 'pending'
                FOR UPDATE SKIP LOCKED
                """,
                job_id,
            )
            if row is None:
                # Either another worker holds the lock right now (SKIP
                # LOCKED returned nothing), or the job is past 'pending'.
                # Both mean the same thing for us: not ours, don't touch.
                return False

            # We hold the row lock — nobody else can claim between the
            # SELECT above and this UPDATE. Flip to 'running' and stamp
            # started_at (same write Phase 1's update_job_running did,
            # now protected by the lock).
            await conn.execute(
                "UPDATE jobs SET status = 'running', started_at = $2 WHERE id = $1",
                job_id,
                datetime.now(timezone.utc).replace(tzinfo=None),
            )
            return True
        # COMMIT happens here as the transaction context exits — this is
        # the instant the row lock releases and other workers see 'running'.


async def mark_processing(redis_client: redis.Redis, job_id: str) -> None:
    """
    Record that a worker is ACTIVELY executing this job right now.

    SADD = Set Add: O(1), idempotent (adding an existing member is a
    no-op — safe even if a retry re-adds the same id). Called immediately
    after a successful claim_job(), BEFORE execution starts.

    WHY THIS EXISTS (crash recovery): BRPOP is destructive — the moment a
    worker pops a job, it's GONE from the queue. If that worker then dies
    (OOM-kill, docker restart, kernel panic) mid-execution, no other
    worker will ever see the job again: it would sit in Postgres as
    'running' forever while nothing actually runs it. This Set is the
    worker's "I'm holding these" ledger — recover_stuck_jobs() reads it
    on startup to find and requeue exactly those orphans.
    """
    await redis_client.sadd(PROCESSING_SET_KEY, job_id)


async def unmark_processing(redis_client: redis.Redis, job_id: str) -> None:
    """
    Remove a job from the in-flight ledger — called on EVERY exit path
    from execution: success, retry-requeued (it's back on the queue, no
    longer in-flight), or moved to DLQ (permanently done). SREM = Set
    Remove: O(1), no-op if the member is already gone.

    A job_id should only ever remain in this Set if a worker died between
    mark and unmark — which is precisely the signal recovery looks for.
    """
    await redis_client.srem(PROCESSING_SET_KEY, job_id)


async def recover_stuck_jobs(pool: asyncpg.Pool, redis_client: redis.Redis) -> int:
    """
    Startup crash-recovery sweep: requeue jobs a dead worker left behind.
    Returns the number of jobs recovered.

    These are jobs that were being processed when a worker crashed — the
    worker SADDed them to jobs:processing, then died before SREMing. We
    requeue them automatically so no job is ever silently lost.

    Runs ONCE, from worker_pool.py, BEFORE any workers start — running it
    per-worker would race (3 workers requeueing the same orphan 3 times).

    HOW EACH ORPHAN IS HANDLED — Postgres is the source of truth, the
    Redis Set is only a hint, so we check the job's REAL status first:
      'running' / 'pending'  -> genuinely orphaned. Rebuild the job dict
          from the Postgres row (id, type, payload, retry_count — the
          same shape the API originally pushed), reset status to
          'pending', LPUSH it back onto the queue, SREM the ledger entry.
      'completed' / 'failed' -> the worker crashed in the narrow window
          AFTER finishing the job but BEFORE SREM. The work is done —
          requeueing would redo a completed side effect (double email!).
          Just SREM the stale entry.
      not in Postgres at all -> Redis has an id Postgres never saw
          (shouldn't happen; defensive). Just SREM.

    AT-LEAST-ONCE, NOT EXACTLY-ONCE: if the worker died between the SMTP
    send and update_job_completed(), the job's status is still 'running',
    so we WILL requeue it and the email WILL go out twice. That's the
    at-least-once delivery guarantee every real queue (SQS, RabbitMQ)
    makes — exactly-once requires idempotent handlers, which is why
    email_handler stamps a message_id (duplicates are detectable).
    """
    # SMEMBERS returns every member of the Set. Fine at this scale (the
    # set only ever holds ~as many entries as there are workers); a system
    # with thousands of in-flight jobs would SSCAN in batches instead.
    stuck_ids = await redis_client.smembers(PROCESSING_SET_KEY)
    if not stuck_ids:
        return 0

    recovered = 0
    for job_id in stuck_ids:
        row = await pool.fetchrow(
            "SELECT id, user_id, type, payload, retry_count, status FROM jobs WHERE id = $1",
            job_id,
        )

        if row is not None:
            status = row["status"]
            if status in ("running", "pending"):
                # Genuine orphan: put it back in line. Reset to 'pending'
                # FIRST (Postgres before Redis, same write-order rule the
                # API follows) so claim_job()'s WHERE status='pending'
                # will accept it when a worker picks it up.
                await pool.execute(
                    "UPDATE jobs SET status = 'pending' WHERE id = $1", job_id
                )
                await redis_client.lpush(
                    QUEUE_KEY,
                    json.dumps({
                        "id": str(row["id"]),
                        "user_id": row["user_id"],
                        "type": row["type"],
                        # payload comes back from asyncpg as a JSON string
                        # (no jsonb codec registered — see api/routes/jobs.py
                        # for the same tradeoff) — decode before re-nesting,
                        # or the worker would receive a string, not a dict.
                        "payload": json.loads(row["payload"]),
                        "retry_count": row["retry_count"] or 0,
                    }),
                )
                log_event("RECOVERED_STUCK_JOB", job_id, extra=f"was={status}, requeued")
                recovered += 1
            else:
                log_event("STALE_PROCESSING_ENTRY", job_id, extra=f"status={status}, dropped")

        # In every branch the ledger entry itself is now handled.
        await redis_client.srem(PROCESSING_SET_KEY, job_id)

    return recovered
