import json 
import asyncpg
from fastapi import APIRouter, Depends, Request,HTTPException

from api.database import get_dlq_count, get_jobs_by_status, get_jobs_last_hour
from api.models import AdminStatsResponse
from api.redis_client import get_queue_depth,push_job 

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
    queue_depth = await get_queue_depth(redis_client)
    jobs_by_status = await get_jobs_by_status(pool)
    dlq_count = await get_dlq_count(pool)
    jobs_last_hour = await get_jobs_last_hour(pool)
    active_jobs = await redis_client.scard("jobs:processing")
    return AdminStatsResponse(
        queue_depth=queue_depth,
        jobs_by_status=jobs_by_status,
        dlq_count=dlq_count,
        jobs_last_hour=jobs_last_hour,
        active_jobs=active_jobs,
    )

@router.get("/admin/dlq")
async def list_dlq_jobs(
    limit: int = 50,
    offset: int =0,
    pool: asyncpg.Pool = Depends(get_db_pool),
) ->dict:
    rows=await pool.fetch(
        """
        SELECT id, original_job_id, job_type, original_payload,
               error_log, created_at
        FROM dead_letter_queue
        ORDER BY created_at DESC
        LIMIT $1 OFFSET $2
        """,
        limit,
        offset,
    )

    return {
        "total": await pool.fetchval("SELECT COUNT(*) FROM dead_letter_queue"),
        "jobs":[
            {
                "dlq_id": str(row["id"]),
                "job_id": str(row["original_job_id"]),
                "type": row["job_type"],
                "payload": json.loads(row["original_payload"]),
                "error_log": row["error_log"],
                "failed_at": row["created_at"].isoformat(),
            }
            for row in rows
        ],
    }

@router.post("/admin/dlq/{job_id}/retry")
async def retry_dlq_job(
    job_id: str,
    pool: asyncpg.Pool=Depends(get_db_pool),
    redis_client= Depends(get_redis),
) ->dict:
    dlq_row=await pool.fetchrow(
      "SELECT job_type, original_payload FROM dead_letter_queue WHERE original_job_id = $1",
        job_id,
    )
    if dlq_row is None:
         raise HTTPException(status_code=404, detail="Job not found in DLQ")
    user_id = await pool.fetchval("SELECT user_id FROM jobs WHERE id = $1", job_id)

    await pool.execute(
        "UPDATE jobs SET status = 'pending', retry_count = 0, error_message = NULL WHERE id = $1",
        job_id,
    )

    await push_job(redis_client, json.dumps({
        "id": job_id,
        "user_id": user_id,
        "type": dlq_row["job_type"],
        "payload": json.loads(dlq_row["original_payload"]),
        "retry_count": 0,   
    }))

    await pool.execute(
        "DELETE FROM dead_letter_queue WHERE original_job_id = $1", job_id
    )

    return {"job_id": job_id, "status": "requeued"}

@router.delete("/admin/dlq/{job_id}")
async def discard_dlq_job(
    job_id: str,
    pool: asyncpg.Pool = Depends(get_db_pool),
) -> dict:
    result = await pool.execute(
        "DELETE FROM dead_letter_queue WHERE original_job_id = $1",
        job_id,
    )

   
    if result == "DELETE 0":
        raise HTTPException(status_code=404, detail="Job not found in DLQ")

    return {"job_id": job_id, "status": "discarded"}

@router.get("/admin/queue-history")
async def get_queue_history(
    pool: asyncpg.Pool = Depends(get_db_pool),
) -> dict:
    rows = await pool.fetch(
        """
        SELECT date_trunc('minute', created_at) AS minute,
               COUNT(*) AS submitted,
               COUNT(*) FILTER (WHERE status = 'completed') AS completed,
               COUNT(*) FILTER (WHERE status = 'failed')    AS failed
        FROM jobs
        WHERE created_at > NOW() - INTERVAL '60 minutes'
        GROUP BY minute
        ORDER BY minute
        """
    )

    return {
        "points": [
            {
                "minute": row["minute"].isoformat(),
                "submitted": row["submitted"],
                "completed": row["completed"],
                "failed": row["failed"],
            }
            for row in rows
        ]
    }

