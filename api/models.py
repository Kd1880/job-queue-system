"""
api/models.py
------------------
PURPOSE: Defines every Pydantic model used to validate incoming HTTP
         requests and shape outgoing HTTP responses for the job queue API.

HOW IT FITS IN THE SYSTEM:
  FastAPI uses these models to automatically validate request bodies BEFORE
  our route handler code ever runs. If a POST /jobs body has a missing
  field, wrong type, or an invalid job "type", FastAPI rejects it with a
  422 response before api/routes/jobs.py even executes — this is why the
  route handlers themselves stay short: validation logic lives here, once,
  instead of being re-checked by hand in every endpoint.

CONTENTS:
  JobType            - allowed job type strings (enum)
  JobStatus          - allowed job lifecycle states (enum)
  EmailPayload / CsvPayload / ImagePayload - per-job-type payload schemas
  JobSubmitRequest   - POST /jobs request body (validates payload per type)
  JobSubmitResponse  - POST /jobs response body
  JobDetailResponse  - GET /jobs/{id} response body
  JobListResponse    - GET /jobs response body (paginated)
  AdminStatsResponse - GET /admin/stats response body
  ErrorResponse      - shape of every error response in the system
"""

from datetime import datetime
from enum import Enum
from typing import Any, List, Optional, Tuple
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field, field_validator, model_validator


class JobType(str, Enum):
    """
    The only three job types Phase 1 supports.

    Using a (str, Enum) instead of a plain string means:
      - FastAPI auto-generates a dropdown of valid values in the /docs UI
      - Pydantic rejects any job type outside this list with a clear 422
        error, instead of the worker discovering an unknown type later
        (fail fast, at the API boundary, not deep inside job execution).
    """
    SEND_EMAIL = "send_email"
    PROCESS_CSV = "process_csv"
    RESIZE_IMAGE = "resize_image"


class JobStatus(str, Enum):
    """
    The full lifecycle a job moves through, in order:
      pending -> running -> completed
                          -> failed (after MAX_RETRIES exhausted)

    Stored as a plain VARCHAR in Postgres (not a native Postgres ENUM — see
    migrations/001_init.sql comment on the `type` column for why), but
    validated as an enum here at the Python/API layer.
    """
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


# ============================================================================
# PER-JOB-TYPE PAYLOAD SCHEMAS
# Each job type has a different shape of `payload`. These models let us
# validate "did the user send the right fields for THIS job type" instead
# of accepting an arbitrary dict and discovering missing fields only when
# the worker crashes trying to execute it.
# ============================================================================

class EmailPayload(BaseModel):
    """Payload required for a send_email job."""
    # EmailStr validates real email-address syntax at the Pydantic layer —
    # catches typos like "not-an-email" before a job even reaches the queue.
    to: EmailStr
    subject: str = Field(..., min_length=1)
    body: str = Field(..., min_length=1)


class CsvPayload(BaseModel):
    """Payload required for a process_csv job."""
    # Just the path to the CSV file (relative to the shared uploads/ volume
    # mounted into both the api and worker containers). We don't validate
    # the file actually exists here — that's the worker's job at execution
    # time, since the file may be uploaded moments after this request.
    file_path: str = Field(..., min_length=1)


class ImagePayload(BaseModel):
    """Payload required for a resize_image job."""
    image_path: str = Field(..., min_length=1)

    # List of (width, height) tuples, e.g. [[800, 600], [400, 300]].
    # min_length=1 ensures the caller requested at least one output size —
    # an empty list would mean "do nothing", which is a caller error.
    sizes: List[Tuple[int, int]] = Field(..., min_length=1)

    @field_validator("sizes")
    @classmethod
    def sizes_must_be_positive(cls, sizes: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
        """Reject nonsensical sizes like (0, 600) or (-100, 100) up front."""
        for width, height in sizes:
            if width <= 0 or height <= 0:
                raise ValueError(f"Invalid size {(width, height)}: width and height must be positive")
        return sizes


# Maps each JobType to the Pydantic model that validates its payload.
# Used by JobSubmitRequest.validate_payload_matches_type below — a single
# lookup table instead of an if/elif chain, so adding a 4th job type later
# means adding one enum value + one payload class + one dict entry here.
_PAYLOAD_SCHEMAS: dict = {
    JobType.SEND_EMAIL: EmailPayload,
    JobType.PROCESS_CSV: CsvPayload,
    JobType.RESIZE_IMAGE: ImagePayload,
}


class JobSubmitRequest(BaseModel):
    """
    Request body for POST /jobs.

    `payload` is deliberately typed as `dict[str, Any]` (not a union of the
    three payload models) because the *correct* schema depends on `type`,
    which we don't know until both fields are present — Pydantic v2 handles
    this "validate field B based on field A" pattern via a model_validator
    that runs after individual field validation.
    """
    user_id: str = Field(..., min_length=1, max_length=100)
    type: JobType
    payload: dict

    @model_validator(mode="after")
    def validate_payload_matches_type(self) -> "JobSubmitRequest":
        """
        Cross-field validation: re-validate `payload` against the specific
        Pydantic schema for `type`.

        WHY THIS RUNS HERE (not in the route handler): if this raised a
        plain Python exception in api/routes/jobs.py, we'd have to
        hand-write a try/except + 400 response there. By raising ValueError
        inside a Pydantic validator instead, FastAPI automatically converts
        it into a structured 422 response with the exact field/message —
        consistent with every other validation error in the app, for free.
        """
        schema = _PAYLOAD_SCHEMAS[self.type]
        try:
            # Re-parse the raw dict through the type-specific schema. This
            # both validates required fields are present AND coerces types
            # (e.g. turns [[800, 600]] JSON arrays into actual tuples).
            schema.model_validate(self.payload)
        except Exception as exc:
            raise ValueError(
                f"Invalid payload for job type '{self.type.value}': {exc}"
            ) from exc
        return self


class JobSubmitResponse(BaseModel):
    """
    Response body for POST /jobs.

    Returned INSTANTLY (before the job actually runs) — this is the whole
    point of a job queue: the caller gets confirmation the job was
    *accepted*, not that it *finished*. Callers poll GET /jobs/{job_id} (or
    later, in Phase 3, a WebSocket) to learn the eventual outcome.
    """
    job_id: UUID
    status: JobStatus
    message: str
    created_at: datetime


class JobDetailResponse(BaseModel):
    """Response body for GET /jobs/{job_id} — the full current state of one job."""
    job_id: UUID
    type: JobType
    status: JobStatus
    payload: dict
    result: Optional[dict] = None
    error_message: Optional[str] = None
    retry_count: int
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class JobListResponse(BaseModel):
    """
    Response body for GET /jobs — a paginated list of jobs for a user.

    Includes `total` (the full count matching the filter, ignoring
    limit/offset) alongside the current page's `jobs`, so a frontend can
    render "showing 1-20 of 143" without a second round-trip request.
    """
    jobs: List[JobDetailResponse]
    total: int
    limit: int
    offset: int


class AdminStatsResponse(BaseModel):
    """Response body for GET /admin/stats — operational snapshot of the whole system."""
    # Number of jobs currently sitting in the Redis "jobs:queue" list,
    # waiting for a worker to pick them up. A steadily growing queue_depth
    # over time means workers can't keep up with submission rate.
    queue_depth: int

    # Count of jobs in Postgres grouped by status, e.g.
    # {"pending": 42, "running": 3, "completed": 150, "failed": 5}.
    jobs_by_status: dict

    # Count of rows in dead_letter_queue — jobs that exhausted all retries.
    dlq_count: int

    # Count of jobs created within the last 60 minutes — a rough
    # throughput/activity indicator.
    jobs_last_hour: int


class ErrorResponse(BaseModel):
    """
    Consistent error shape returned by every endpoint in the system,
    regardless of failure type (validation, not-found, server error).
    A frontend only ever needs to handle ONE error shape, not a different
    one per endpoint.
    """
    error: str
    detail: Optional[str] = None
