"""
api/main.py
------------------
PURPOSE: The FastAPI application entry point. Wires together the database
         pool, the Redis client, the route modules, and consistent
         error-handling for the entire API surface.

HOW IT FITS IN THE SYSTEM:
  This is the process started by `uvicorn api.main:app` (see
  api/Dockerfile and docker-compose.yml's `command:` for the api service).
  It is a completely separate OS process from the worker (worker/worker.py)
  — the two only ever communicate indirectly, through Redis (the queue) and
  Postgres (job state), never through direct function calls or shared
  memory. This decoupling is the core idea of a job queue architecture: the
  API can crash/restart/deploy independently of the worker, and vice versa.

STARTUP/SHUTDOWN LIFECYCLE:
  On startup: open one shared asyncpg connection pool and one shared Redis
  client, both stored on `app.state` so every request handler can reuse
  them (see api/routes/jobs.py's `get_db_pool`/`get_redis` dependencies).
  On shutdown: close both cleanly so Postgres/Redis see graceful
  disconnects instead of the app just vanishing.
"""
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from api.database import close_pool, create_pool
from api.middleware.rate_limiter import RateLimitExceeded
from api.redis_client import close_redis_client, create_redis_client
from api.routes import admin, jobs
from api.websocket_manager import manager 


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI's modern startup/shutdown hook (replaces the older
    @app.on_event("startup") / ("shutdown") decorators).

    Code before `yield` runs once when the app starts; code after `yield`
    runs once when the app is shutting down (e.g. on `docker-compose down`
    sending SIGTERM). Everything in between is the app actually serving
    requests.
    """
    # STARTUP: open the shared Postgres pool and Redis client ONCE. If we
    # instead created a new connection per-request, we'd exhaust Postgres's
    # connection limit under any real load and pay TCP+auth handshake
    # latency on every single API call.
    app.state.db_pool = await create_pool()
    app.state.redis_client = create_redis_client()
    app.state.pubsub_task=asyncio.create_task(
        manager.subscribe_to_redis(app.state.redis_client)
    )

    yield  # <-- the app serves requests here
    app.state.pubsub_task.cancel()

    # SHUTDOWN: close both cleanly so Postgres/Redis see a graceful
    # disconnect (freeing their server-side resources immediately) instead
    # of waiting for a TCP timeout to notice the client vanished.
    await close_pool(app.state.db_pool)
    await close_redis_client(app.state.redis_client)


app = FastAPI(
    title="Job Queue System API",
    description="Accepts job submissions and reports on job status.",
    version="1.0.0",
    lifespan=lifespan,
)

# Mount the two route modules. Each APIRouter groups related endpoints —
# jobs.router owns POST/GET /jobs*, admin.router owns GET /admin/stats.
# Splitting by concern (not by HTTP verb) keeps each file focused on one
# part of the domain.
app.include_router(jobs.router)
app.include_router(admin.router)


# ============================================================================
# GLOBAL ERROR HANDLERS
# Every error response in the system, regardless of where it originates,
# comes back in the SAME shape: {"error": "...", "detail": "..." }. A
# frontend integrating with this API only ever needs to handle one error
# shape, instead of a different one per endpoint or failure type.
# ============================================================================

@app.exception_handler(RequestValidationError)
async def validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """
    Catches every Pydantic validation failure — malformed JSON, missing
    required fields, wrong types, or a JobSubmitRequest whose payload
    doesn't match its declared job type (see the model_validator in
    api/models.py::JobSubmitRequest). FastAPI's default behavior for these
    is a 422 response in its own bespoke shape; we intercept it here so it
    matches every other error response in the system instead.
    """
    # exc.errors() is a list of Pydantic error dicts; joining their
    # messages gives a single human-readable detail string rather than
    # forcing API consumers to parse a nested error-list structure.
    detail = "; ".join(f"{'.'.join(str(loc) for loc in e['loc'])}: {e['msg']}" for e in exc.errors())
    return JSONResponse(
        status_code=422,
        content={"error": "Validation failed", "detail": detail},
    )


@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request: Request, exc: RateLimitExceeded) -> JSONResponse:
    """
    PHASE 2: Converts a RateLimitExceeded (raised by the check_rate_limit
    dependency on POST /jobs) into the documented 429 response.

    429 = "Too Many Requests", the standard rate-limiting status code.
    The Retry-After header is the machine-readable version of the same
    hint as the retry_after body field — standard HTTP clients (and
    well-behaved SDK retry policies) honor the header automatically,
    while humans reading JSON in a console see the body field.
    """
    return JSONResponse(
        status_code=429,
        content={
            "error": "Rate limit exceeded",
            "limit": exc.limit,
            "window": "1 minute",
            "retry_after": exc.retry_after,
        },
        headers={"Retry-After": str(exc.retry_after)},
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """
    Catches every explicit `raise HTTPException(...)` from route handlers
    (e.g. the 404 in GET /jobs/{job_id}, the 400s in list_user_jobs) and
    reshapes FastAPI's default {"detail": "..."} body into our consistent
    {"error": "...", "detail": "..."} shape.
    """
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.detail, "detail": None},
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """
    Last-resort catch-all for anything that isn't an HTTPException or a
    validation error — e.g. Postgres briefly unreachable, Redis connection
    dropped mid-request. Ensures the caller ALWAYS gets valid JSON back
    (never a raw stack trace or a connection reset), while the real
    exception message still reaches server logs via `print` for debugging.
    """
    print(f"[UNHANDLED ERROR] {type(exc).__name__}: {exc}")
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "detail": str(exc)},
    )


@app.get("/")
async def root() -> dict:
    """Trivial liveness check — lets you confirm the container is up with `curl localhost:8000/`."""
    return {"service": "job-queue-api", "status": "ok"}

@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket:WebSocket, user_id:str):
    await manager.connect(websocket, user_id)

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket, user_id)