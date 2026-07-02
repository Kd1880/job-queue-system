"""
api/database.py
------------------
PURPOSE: Owns every direct interaction between the FastAPI app and
         Postgres. Nothing outside this file writes raw SQL — routes call
         these functions instead, so the SQL for "how do I fetch a job" or
         "how do I count jobs by status" exists in exactly one place.

HOW IT FITS IN THE SYSTEM:
  Postgres is the PERMANENT, SOURCE-OF-TRUTH store for job state (see
  migrations/001_init.sql). The API writes here the moment a job is
  submitted (status='pending') and reads here to answer every GET request.
  The worker (a completely separate process/container) also talks directly
  to Postgres — via its own copy of similar functions in worker/utils.py,
  since the worker's Docker build context does not include api/ code.

CONNECTION POOLING:
  We use asyncpg's connection pool (not a single connection) because
  FastAPI handles many concurrent HTTP requests, and each one needs its own
  DB connection for the duration of a query. A pool of ~5-20 reusable
  connections is far cheaper than opening a brand-new TCP connection to
  Postgres on every single request.
"""

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import asyncpg

# Read once at import time. DATABASE_URL is injected via docker-compose's
# env_file (see .env) — same value the worker uses, so both processes agree
# on exactly which Postgres instance/database they're talking to.
DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/jobqueue"
)


async def create_pool() -> asyncpg.Pool:
    """
    Create the asyncpg connection pool used for the lifetime of the app.

    Called once in api/main.py's startup event and stashed on app.state,
    so every request handler reuses the same pool instead of each one
    opening its own connection (which would exhaust Postgres's max
    connection limit under load).

    min_size/max_size: keep a small number of connections warm (min_size)
    so the very first requests after startup don't pay TCP+auth handshake
    latency, while capping at max_size so a traffic spike can't open
    unbounded connections and overwhelm Postgres.
    """
    return await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)


async def close_pool(pool: asyncpg.Pool) -> None:
    """Gracefully close every connection in the pool on app shutdown."""
    await pool.close()


async def insert_job(
    pool: asyncpg.Pool,
    job_id: str,
    user_id: str,
    job_type: str,
    payload: dict,
) -> datetime:
    """
    Insert a brand-new job row with status='pending'.

    WHY WE INSERT TO POSTGRES BEFORE PUSHING TO REDIS (enforced by the
    caller in api/routes/jobs.py, not here): if we pushed to Redis first
    and this insert then failed, a worker could pick up a job that has no
    corresponding database row to update — it would have nowhere to record
    "completed" or "failed". Writing to permanent storage first means the
    worst case is a Postgres row stuck at 'pending' forever (visible,
    debuggable), never a job silently running with no record of it existing
    at all. Returns the exact created_at timestamp that Postgres assigned.

    Uses the primary key + a single-row INSERT — O(1) write, no scan
    involved. `RETURNING created_at` avoids a second round-trip query to
    fetch the timestamp Postgres just generated via DEFAULT NOW().
    """
    row = await pool.fetchrow(
        """
        INSERT INTO jobs (id, user_id, type, payload, status)
        VALUES ($1, $2, $3, $4::jsonb, 'pending')
        RETURNING created_at
        """,
        job_id,
        user_id,
        job_type,
        json.dumps(payload),
    )
    return row["created_at"]


async def get_job(pool: asyncpg.Pool, job_id: str) -> Optional[asyncpg.Record]:
    """
    Fetch a single job by its UUID primary key.

    Primary-key lookup — O(1) via Postgres's automatic PK index, no need
    for any of the secondary indexes defined in the migration. Returns
    None (not an exception) when no row matches, so callers decide how to
    surface "not found" (the route turns this into a 404).
    """
    return await pool.fetchrow(
        """
        SELECT id, user_id, type, payload, status, result, error_message,
               retry_count, created_at, started_at, completed_at
        FROM jobs
        WHERE id = $1
        """,
        job_id,
    )


async def list_jobs(
    pool: asyncpg.Pool,
    user_id: str,
    status: Optional[str],
    limit: int,
    offset: int,
) -> tuple[list[asyncpg.Record], int]:
    """
    Fetch a paginated, optionally status-filtered list of jobs for one
    user, newest first, plus the total count matching the filter
    (ignoring limit/offset) for pagination UI.

    Uses idx_jobs_user_status (when status is given) or idx_jobs_user_id
    (when it isn't) to avoid a full table scan — see migrations/001_init.sql
    for why those composite/single indexes exist. ORDER BY created_at DESC
    is satisfied directly from idx_jobs_created_at without an extra sort.

    Runs two queries (page + count) rather than one window-function query
    (COUNT(*) OVER()) for readability; at Phase 1's scale the extra query
    is negligible, and it keeps the SQL simple to reason about.
    """
    if status:
        rows = await pool.fetch(
            """
            SELECT id, user_id, type, payload, status, result, error_message,
                   retry_count, created_at, started_at, completed_at
            FROM jobs
            WHERE user_id = $1 AND status = $2
            ORDER BY created_at DESC
            LIMIT $3 OFFSET $4
            """,
            user_id,
            status,
            limit,
            offset,
        )
        total_row = await pool.fetchrow(
            "SELECT COUNT(*) AS total FROM jobs WHERE user_id = $1 AND status = $2",
            user_id,
            status,
        )
    else:
        rows = await pool.fetch(
            """
            SELECT id, user_id, type, payload, status, result, error_message,
                   retry_count, created_at, started_at, completed_at
            FROM jobs
            WHERE user_id = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            """,
            user_id,
            limit,
            offset,
        )
        total_row = await pool.fetchrow(
            "SELECT COUNT(*) AS total FROM jobs WHERE user_id = $1",
            user_id,
        )

    return rows, total_row["total"]


async def get_jobs_by_status(pool: asyncpg.Pool) -> dict[str, int]:
    """
    Count jobs grouped by status, for GET /admin/stats.

    GROUP BY status uses idx_jobs_status to avoid scanning every row
    individually — Postgres can walk the index rather than the heap.
    Returns a dict like {"pending": 42, "running": 3, ...} with every
    known JobStatus present (defaulting to 0) so API consumers never have
    to handle a missing key.
    """
    rows = await pool.fetch("SELECT status, COUNT(*) AS count FROM jobs GROUP BY status")
    counts = {"pending": 0, "running": 0, "completed": 0, "failed": 0}
    for row in rows:
        counts[row["status"]] = row["count"]
    return counts


async def get_dlq_count(pool: asyncpg.Pool) -> int:
    """
    Count of jobs that permanently failed (exhausted all retries) and were
    moved to dead_letter_queue. Full count, no index needed — this table
    is expected to stay small relative to `jobs`.
    """
    row = await pool.fetchrow("SELECT COUNT(*) AS count FROM dead_letter_queue")
    return row["count"]


async def get_jobs_last_hour(pool: asyncpg.Pool) -> int:
    """
    Count of jobs created in the last 60 minutes — a rough throughput
    indicator for GET /admin/stats.

    idx_jobs_created_at (DESC) makes this a fast range scan from "now"
    backwards, rather than a full table scan.
    """
    one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
    row = await pool.fetchrow(
        "SELECT COUNT(*) AS count FROM jobs WHERE created_at >= $1",
        one_hour_ago.replace(tzinfo=None),
    )
    return row["count"]
