"""
worker/utils.py
------------------
PURPOSE: Shared utilities for the worker process — everything that isn't
         "how do I execute this specific job type" lives here: connecting
         to Postgres/Redis, and every read/write query the worker needs to
         move a job through its lifecycle (pending -> running -> completed
         | failed -> dead_letter_queue).

WHY THIS DUPLICATES SOME OF api/database.py / api/redis_client.py:
  The worker's Docker image (see worker/Dockerfile) only COPYs the
  worker/ directory into the container — it never includes api/. This is
  intentional: the api and worker are independently deployable services in
  a real distributed system (Phase 2 will scale them to different replica
  counts), so they must not share a Python import path. A small amount of
  duplicated connection-setup code is the price of that independence, and
  is far preferable to a fragile shared-package dependency between two
  services that are supposed to be decoupled.

HOW IT FITS IN THE SYSTEM:
  worker/worker.py (the main loop) calls into this module for every
  database read/write and every Redis interaction beyond the initial
  BRPOP. Keeping SQL/Redis calls here (not inline in the loop) keeps the
  main loop readable as pure orchestration logic.
"""

import json
import os
from datetime import datetime, timezone
from typing import Any, Optional

import asyncpg
import redis.asyncio as redis

# Same env vars as api/database.py and api/redis_client.py — both read from
# the identical .env file via docker-compose's env_file, so api and worker
# always agree on which Postgres/Redis instance to talk to.
DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/jobqueue"
)
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")

# Must match api/redis_client.py's QUEUE_KEY exactly — this is the one
# Redis list both processes agree to use as the handoff point. If these
# ever drifted apart, jobs pushed by the API would never be seen by the
# worker (silently, with no error on either side) — a good reason this
# constant, like DATABASE_URL/REDIS_URL, is worth keeping trivially easy to
# grep for across the codebase.
QUEUE_KEY = "jobs:queue"

# Read from the environment so `docker-compose.yml`'s .env is the single
# place retry behavior is tuned, without editing code.
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
WORKER_TIMEOUT = int(os.environ.get("WORKER_TIMEOUT", "5"))


async def create_db_pool() -> asyncpg.Pool:
    """
    Create the worker's own asyncpg connection pool.

    min_size/max_size are small (the worker processes ONE job at a time in
    Phase 1 — see worker/worker.py's single `while True` loop with no
    concurrency), so it never needs more than a couple of connections. This
    will grow in Phase 2 when multiple worker processes/coroutines run
    concurrently.
    """
    return await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)


def create_redis_client() -> redis.Redis:
    """
    Create the worker's Redis client.

    decode_responses=True so BRPOP and every other Redis call returns
    plain Python str instead of bytes — the worker immediately
    json.loads() whatever it reads, so raw bytes would just need decoding
    by hand first.
    """
    return redis.from_url(REDIS_URL, decode_responses=True)


def log_event(event: str, job_id: str, extra: str = "") -> None:
    """
    Print a single structured lifecycle log line.

    WHY PRINT (not a logging framework) in Phase 1: the worker runs as a
    Docker container with stdout captured by `docker-compose logs -f
    worker` — print() is instantly visible there with zero configuration.
    A real logging framework (structured JSON logs, log levels, shipping
    to a log aggregator) is a natural Phase 2+ upgrade once this system
    has more than one worker to correlate logs across.

    Every job lifecycle transition funnels through this one function so
    the log format stays consistent: [TIMESTAMP] EVENT job_id=... extra
    """
    timestamp = datetime.now(timezone.utc).isoformat()
    suffix = f" {extra}" if extra else ""
    print(f"[{timestamp}] {event} job_id={job_id}{suffix}", flush=True)


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
    """
    Mark a job as 'running' and stamp started_at with the current time.

    WHY WE WRITE THIS BEFORE EXECUTING (not after): if the worker process
    crashed mid-execution with no 'running' checkpoint ever written, a
    later inspection of the jobs table couldn't distinguish "never
    started" from "started and crashed". Recording 'running' + started_at
    up front means started_at is always an honest timestamp of when work
    actually began, and a job stuck in 'running' for a suspiciously long
    time is a visible signal something went wrong (a stuck-job sweep is a
    natural Phase 2 addition).
    """
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
