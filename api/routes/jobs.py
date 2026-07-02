"""
api/routes/jobs.py
------------------
PURPOSE: Handles all job-related HTTP endpoints.
         This is the entry point for users submitting jobs.

HOW IT FITS IN THE SYSTEM:
  User -> POST /jobs here -> we validate -> write to Postgres -> push to
          Redis queue -> return job_id instantly (worker executes later)
  User -> GET /jobs/{id} here -> we fetch from Postgres -> return current
          status (pending / running / completed / failed)
  User -> GET /jobs here -> we fetch a paginated, filterable list of a
          user's jobs from Postgres

ENDPOINTS:
  POST   /jobs           - Submit a new job
  GET    /jobs/{job_id}  - Get status of a specific job
  GET    /jobs            - List jobs for a user (paginated, filterable)
"""

import json
import uuid
from typing import Optional

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query, Request

from api.database import get_job, insert_job, list_jobs
from api.models import JobDetailResponse, JobListResponse, JobSubmitRequest, JobSubmitResponse
from api.redis_client import push_job

router = APIRouter(tags=["jobs"])


def get_db_pool(request: Request) -> asyncpg.Pool:
    """
    FastAPI dependency that hands route handlers the shared asyncpg pool.

    The pool itself is created once at app startup (see api/main.py) and
    stashed on `app.state.db_pool` — this dependency just retrieves it, so
    every request reuses existing connections instead of opening new ones.
    """
    return request.app.state.db_pool


def get_redis(request: Request):
    """Same pattern as get_db_pool, but for the shared Redis client."""
    return request.app.state.redis_client


def _record_to_job_detail(record: asyncpg.Record) -> JobDetailResponse:
    """
    Convert a raw asyncpg.Record (one row from the `jobs` table) into a
    JobDetailResponse the API can serialize to JSON.

    WHY THIS CONVERSION IS NEEDED: asyncpg returns JSONB columns
    (`payload`, `result`) as plain JSON-text strings, not parsed Python
    dicts — asyncpg doesn't know we want them decoded unless we register a
    codec on the connection. It's simpler and more explicit to json.loads()
    them by hand right here, once, than to configure a global codec that
    silently changes the type of every jsonb column app-wide.
    """
    return JobDetailResponse(
        job_id=record["id"],
        type=record["type"],
        status=record["status"],
        payload=json.loads(record["payload"]),
        result=json.loads(record["result"]) if record["result"] is not None else None,
        error_message=record["error_message"],
        retry_count=record["retry_count"],
        created_at=record["created_at"],
        started_at=record["started_at"],
        completed_at=record["completed_at"],
    )


@router.post("/jobs", response_model=JobSubmitResponse, status_code=201)
async def submit_job(
    request: JobSubmitRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    redis_client=Depends(get_redis),
) -> JobSubmitResponse:
    """
    Submit a new job to the queue.

    FLOW:
      1. Validate job type and payload fields
         -> Already done by the time this function runs: FastAPI validated
            the request body against JobSubmitRequest (api/models.py)
            BEFORE calling this handler. If validation had failed, the
            caller would have already received a 422 response.
      2. Generate a unique job ID
      3. Insert job into Postgres (status = 'pending')
         -> Postgres is permanent storage — job record lives here forever
      4. Push job JSON to Redis list "jobs:queue" via LPUSH
         -> Redis is the fast handoff to workers — in-memory, microsecond
            speed
      5. Return job_id instantly - DO NOT wait for job to execute
         -> This is the whole point: user gets a response in <100ms
            regardless of whether the job itself takes 1 second or 10
            minutes to actually run

    WHY WE INSERT TO POSTGRES FIRST, THEN REDIS:
      If we pushed to Redis first and the Postgres insert failed, we'd have
      a job sitting in the queue with no record in the database. A worker
      could pick it up but have nowhere to write status updates. Always
      write to permanent storage before handing off to the transient queue.

    ARGS:
      request: Validated request body (Pydantic/FastAPI handles validation
               automatically via JobSubmitRequest)
      pool: Shared asyncpg connection pool (injected)
      redis_client: Shared Redis client (injected)

    RETURNS:
      JobSubmitResponse with job_id, status='pending', created_at
    """
    # Step 1: Generate unique job ID.
    # UUID4 = random UUID -> no two jobs will ever collide. This exact ID
    # is used everywhere downstream: Postgres primary key, Redis payload,
    # and the API response the caller uses to poll for status later.
    job_id = str(uuid.uuid4())

    # Step 2: Insert into Postgres FIRST (permanent record).
    # status='pending' means: the job exists but no worker has started it
    # yet. started_at/completed_at stay NULL until the worker reaches those
    # stages. See api/database.py::insert_job for the full SQL + rationale.
    created_at = await insert_job(
        pool, job_id, request.user_id, request.type.value, request.payload
    )

    # Step 3: Build the exact job dict that travels through Redis to the
    # worker. Includes retry_count=0 up front so the worker never has to
    # special-case "first attempt" vs. "retried attempt" — the field is
    # always present.
    job_data = {
        "id": job_id,
        "user_id": request.user_id,
        "type": request.type.value,
        "payload": request.payload,
        "retry_count": 0,
    }

    # Step 4: Push to Redis queue (fast handoff to the worker).
    # LPUSH = Left Push, worker consumes via BRPOP (Right Pop) -> FIFO
    # order (first submitted, first processed). See api/redis_client.py
    # for the full rationale and failure-mode discussion.
    await push_job(redis_client, json.dumps(job_data))

    # Step 5: Return immediately. The worker will pick this up
    # asynchronously — the caller does not wait for job execution.
    return JobSubmitResponse(
        job_id=job_id,
        status="pending",
        message="Job queued successfully",
        created_at=created_at,
    )


@router.get("/jobs/{job_id}", response_model=JobDetailResponse)
async def get_job_status(
    job_id: str,
    pool: asyncpg.Pool = Depends(get_db_pool),
) -> JobDetailResponse:
    """
    Fetch the current state of a single job.

    FLOW:
      1. Validate job_id is a well-formed UUID (400 if not — a malformed
         ID can never match a row, so there's no point querying Postgres)
      2. Look up the job by primary key
         -> O(1) via Postgres's automatic PK index
      3. 404 if no matching row exists
      4. Otherwise return the full current row: this is how a caller polls
         "is my job done yet" — status will read pending -> running ->
         completed (or failed, with error_message populated)
    """
    # Step 1: Reject malformed UUIDs before touching the database at all.
    try:
        uuid.UUID(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"'{job_id}' is not a valid job ID")

    # Step 2: Primary-key lookup. See api/database.py::get_job.
    record = await get_job(pool, job_id)

    # Step 3: No row found -> the job never existed (or the ID is simply
    # wrong) -> 404, the standard HTTP status for "resource not found".
    if record is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

    # Step 4: Shape the row into the response model.
    return _record_to_job_detail(record)


@router.get("/jobs", response_model=JobListResponse)
async def list_user_jobs(
    user_id: str = Query(..., min_length=1, description="Filter jobs by user"),
    status: Optional[str] = Query(
        None, description="Optionally filter by job status (pending/running/completed/failed)"
    ),
    limit: int = Query(20, ge=1, le=100, description="Max number of jobs to return"),
    offset: int = Query(0, ge=0, description="Number of jobs to skip (for pagination)"),
    pool: asyncpg.Pool = Depends(get_db_pool),
) -> JobListResponse:
    """
    List jobs belonging to a user, newest first, with optional status
    filtering and pagination.

    FLOW:
      1. Validate `status`, if provided, is one of the four known values
         (400 if not — an unrecognized status can never match any row).
      2. Query Postgres for the matching page of rows PLUS the total count
         matching the filter (ignoring limit/offset), so the caller can
         render pagination UI without a second request.
      3. Shape each row into a JobDetailResponse and return the page.

    `limit` is capped at 100 (via Query(..., le=100)) to prevent a caller
    from accidentally requesting the entire jobs table in one response.
    """
    # Step 1: Validate status against the known set. Doing this here (not
    # relying on Postgres to just return zero rows for a typo'd status)
    # gives the caller a clear 400 instead of a silently-empty result set
    # that looks identical to "you have no jobs".
    valid_statuses = {"pending", "running", "completed", "failed"}
    if status is not None and status not in valid_statuses:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid status '{status}'. Must be one of: {sorted(valid_statuses)}",
        )

    # Step 2: Fetch the page + total count. See api/database.py::list_jobs
    # for index usage and query rationale.
    records, total = await list_jobs(pool, user_id, status, limit, offset)

    # Step 3: Shape rows into response models.
    jobs = [_record_to_job_detail(record) for record in records]

    return JobListResponse(jobs=jobs, total=total, limit=limit, offset=offset)
