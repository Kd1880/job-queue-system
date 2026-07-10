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
 
    SEND_EMAIL = "send_email"
    PROCESS_CSV = "process_csv"
    RESIZE_IMAGE = "resize_image"
    # Phase 2 additions:
    IMAGE_PROCESSOR = "image_processor"
    DATA_PIPELINE = "data_pipeline"


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class EmailPayload(BaseModel):
    """Payload required for a send_email job."""
    # EmailStr validates real email-address syntax at the Pydantic layer —
    # catches typos like "not-an-email" before a job even reaches the queue.
    to: EmailStr
    subject: str = Field(..., min_length=1)
    body: str = Field(..., min_length=1)

    # PHASE 2: optional HTML version of the same message. When present, the
    # worker sends a multipart/alternative email (plain + HTML together) —
    # modern clients render the HTML, ancient ones fall back to plain text.
    # Optional with default None so every Phase 1 payload remains valid.
    html_body: Optional[str] = None


class CsvPayload(BaseModel):

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


class ImageProcessorPayload(BaseModel):
    
    image_path: str = Field(..., min_length=1)

    # Which pipeline stages to run, e.g. ["resize", "compress", "convert",
    # "thumbnail"]. Lets a caller run a subset (thumbnail only) without a
    # separate job type per combination.
    operations: List[str] = Field(..., min_length=1)

    # Target bounding boxes for the resize stage, e.g. [[800,600],[400,300]].
    # Optional because a thumbnail-only job has no resize dimensions.
    resize_dimensions: Optional[List[Tuple[int, int]]] = None

    # Output format for converted images. WebP default: ~30% smaller than
    # JPEG at equivalent visual quality (better entropy coding), supported
    # by every modern browser.
    convert_to: str = "webp"

    # Lossy compression quality 1-100. 85 is the sweet spot used by most
    # CDNs: visually indistinguishable from 100 at roughly half the bytes.
    quality: int = Field(85, ge=1, le=100)

    generate_thumbnail: bool = False
    thumbnail_size: Tuple[int, int] = (150, 150)

    @field_validator("operations")
    @classmethod
    def operations_must_be_known(cls, operations: List[str]) -> List[str]:
        """Reject typos like 'reize' at the API boundary, not in the worker."""
        allowed = {"resize", "compress", "convert", "thumbnail"}
        unknown = set(operations) - allowed
        if unknown:
            raise ValueError(f"Unknown operations {sorted(unknown)}. Allowed: {sorted(allowed)}")
        return operations

    @field_validator("resize_dimensions")
    @classmethod
    def dimensions_must_be_positive(
        cls, dims: Optional[List[Tuple[int, int]]]
    ) -> Optional[List[Tuple[int, int]]]:
        """Same guard as ImagePayload.sizes — no zero/negative boxes."""
        if dims is not None:
            for width, height in dims:
                if width <= 0 or height <= 0:
                    raise ValueError(f"Invalid dimensions {(width, height)}: must be positive")
        return dims


class DataPipelinePayload(BaseModel):
    """
    Payload for a Phase 2 data_pipeline job — the full ETL version of
    process_csv: validate columns, clean (duplicates/nulls/whitespace),
    transform, compute stats and a 0-100 quality score.
    """
    file_path: str = Field(..., min_length=1)

    # Which ETL stages to run, e.g. ["validate", "clean", "transform", "stats"].
    operations: List[str] = Field(..., min_length=1)

    expected_columns: Optional[List[str]] = None

    drop_duplicates: bool = True

    # 'drop' = remove rows containing nulls; 'fill' = replace nulls with a
    # sensible default (empty string / column mean). Payload-driven so the
    # same handler serves both strict and lenient cleaning policies.
    handle_nulls: str = Field("drop", pattern="^(drop|fill)$")

    output_format: str = Field("csv", pattern="^(csv|json)$")

    @field_validator("operations")
    @classmethod
    def operations_must_be_known(cls, operations: List[str]) -> List[str]:
        allowed = {"validate", "clean", "transform", "stats"}
        unknown = set(operations) - allowed
        if unknown:
            raise ValueError(f"Unknown operations {sorted(unknown)}. Allowed: {sorted(allowed)}")
        return operations


# Maps each JobType to the Pydantic model that validates its payload.
# Used by JobSubmitRequest.validate_payload_matches_type below — a single
# lookup table instead of an if/elif chain, so adding a new job type later
# means adding one enum value + one payload class + one dict entry here.
_PAYLOAD_SCHEMAS: dict = {
    JobType.SEND_EMAIL: EmailPayload,
    JobType.PROCESS_CSV: CsvPayload,
    JobType.RESIZE_IMAGE: ImagePayload,
    JobType.IMAGE_PROCESSOR: ImageProcessorPayload,
    JobType.DATA_PIPELINE: DataPipelinePayload,
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
    # Number of jobs being executed by workers RIGHT NOW — the live size
    # of the Redis "jobs:processing" set (Phase 2's crash-recovery ledger,
    # reused here as a busy-ness signal). 0 = all workers idle.
    active_jobs: int



class ErrorResponse(BaseModel):
    """
    Consistent error shape returned by every endpoint in the system,
    regardless of failure type (validation, not-found, server error).
    A frontend only ever needs to handle ONE error shape, not a different
    one per endpoint.
    """
    error: str
    detail: Optional[str] = None
