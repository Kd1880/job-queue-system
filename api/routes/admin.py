"""
api/routes/admin.py
------------------
PURPOSE: Operational visibility into the health of the whole job queue
         system — a single endpoint an operator (or a monitoring
         dashboard) can poll to answer "is the system keeping up?"

HOW IT FITS IN THE SYSTEM:
  Pulls from BOTH data stores in one response:
    - Redis  -> queue_depth (jobs waiting, not yet picked up by a worker)
    - Postgres -> jobs_by_status, dlq_count, jobs_last_hour (historical +
                  current state of everything that's ever been submitted)
  A steadily growing queue_depth alongside a steady jobs_by_status.pending
  count is the classic signal that workers can't keep up with submission
  rate — exactly the kind of thing Phase 2 (multiple workers) exists to fix.

ENDPOINTS:
  GET /admin/stats - Snapshot of queue depth, job counts by status, DLQ
                      size, and recent throughput.
"""

import asyncpg
from fastapi import APIRouter, Depends, Request

from api.database import get_dlq_count, get_jobs_by_status, get_jobs_last_hour
from api.models import AdminStatsResponse
from api.redis_client import get_queue_depth

router = APIRouter(tags=["admin"])


def get_db_pool(request: Request) -> asyncpg.Pool:
    """Same dependency pattern as api/routes/jobs.py — reuse the shared pool."""
    return request.app.state.db_pool


def get_redis(request: Request):
    """Same dependency pattern as api/routes/jobs.py — reuse the shared client."""
    return request.app.state.redis_client


@router.get("/admin/stats", response_model=AdminStatsResponse)
async def get_stats(
    pool: asyncpg.Pool = Depends(get_db_pool),
    redis_client=Depends(get_redis),
) -> AdminStatsResponse:
    """
    Return a point-in-time snapshot of system health.

    FLOW:
      1. LLEN the Redis queue -> how many jobs are waiting right now,
         un-popped by any worker (see api/redis_client.py::get_queue_depth)
      2. GROUP BY status in Postgres -> how many jobs are in each lifecycle
         stage across all of history (see api/database.py::get_jobs_by_status)
      3. COUNT the dead_letter_queue table -> jobs that permanently failed
      4. COUNT jobs created in the last hour -> rough throughput indicator

    These four numbers are independent queries against two different data
    stores — there's no transactional consistency between them (e.g.
    queue_depth might reflect a slightly different instant than
    jobs_by_status.pending). That's acceptable for an operational
    dashboard: we want a fast, cheap snapshot, not a perfectly
    linearizable one.
    """
    queue_depth = await get_queue_depth(redis_client)
    jobs_by_status = await get_jobs_by_status(pool)
    dlq_count = await get_dlq_count(pool)
    jobs_last_hour = await get_jobs_last_hour(pool)

    return AdminStatsResponse(
        queue_depth=queue_depth,
        jobs_by_status=jobs_by_status,
        dlq_count=dlq_count,
        jobs_last_hour=jobs_last_hour,
    )
