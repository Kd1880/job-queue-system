# Distributed Job Queue System — Phase 1

A background job processing pipeline: submit a job over HTTP, get an
instant response, and let a worker process execute it asynchronously.
Built as a portfolio project demonstrating core distributed-systems
concepts: async task queues, at-least-once processing with retries,
idempotency, and a dead letter queue.

## Architecture

```
                POST /jobs                    LPUSH
   Client  ───────────────▶  FastAPI  ───────────────▶  Redis
                              (api)      "jobs:queue"    (queue)
                                │                            │
                                │ INSERT                     │ BRPOP
                                ▼                            ▼
                            Postgres  ◀──── UPDATE ────  Worker
                          (source of truth)              (worker)
```

- **FastAPI (`api/`)** accepts job submissions, validates them, writes a
  permanent record to Postgres, and pushes the job onto a Redis queue —
  all before responding. The client gets a response in milliseconds; it
  never waits for the job to actually run.
- **Redis** is a fast, transient handoff queue (`jobs:queue`, a Redis
  LIST). It holds a job only from submission until a worker pops it.
- **Worker (`worker/`)** runs as its own process, blocking on Redis
  (`BRPOP`) for new jobs, executing them, and writing results back to
  Postgres. It retries failed jobs with exponential backoff and moves
  permanently-failed jobs to a dead letter queue.
- **Postgres** is the permanent source of truth for every job's full
  history and current state.

## Job types

| Type            | What it does                                                             |
|-----------------|---------------------------------------------------------------------------|
| `send_email`    | Mocked — logs + sleeps 1s to simulate an email API call                  |
| `process_csv`   | Real — reads a CSV with pandas, removes duplicate rows, saves the result |
| `resize_image`  | Real — resizes an image to multiple sizes with Pillow, preserving aspect ratio |

See `api/models.py` for the exact payload schema each type requires.

## Quickstart

Requires Docker and Docker Compose.

```bash
# 1. Create your local .env from the template (holds dev-only defaults)
cp .env.example .env

# 2. Boot the whole system
docker-compose up
```

This builds the `api` and `worker` images, starts Postgres and Redis,
waits for both to report healthy, runs the schema migration
(`migrations/001_init.sql`) automatically on Postgres's first boot, and
then starts the API (port 8000) and worker.

Once it's up:

```bash
# Submit a job
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{
        "user_id": "user-123",
        "type": "send_email",
        "payload": {"to": "someone@example.com", "subject": "Hi", "body": "Hello!"}
      }'
# -> {"job_id": "...", "status": "pending", "message": "Job queued successfully", "created_at": "..."}

# Poll its status (watch it move pending -> running -> completed)
curl http://localhost:8000/jobs/<job_id>

# List a user's jobs
curl "http://localhost:8000/jobs?user_id=user-123&status=completed&limit=20&offset=0"

# System-wide operational stats
curl http://localhost:8000/admin/stats
```

Interactive API docs (Swagger UI) are available at
`http://localhost:8000/docs` once the stack is running.

### Seed some realistic test data

With the stack running, from the repo root on your host machine:

```bash
python scripts/seed_test_data.py
```

This generates a 500-row CSV (with duplicates) and a sample image under
`uploads/`, submits 5 jobs of mixed types, and prints each `job_id` so you
can immediately poll them.

## Project layout

```
job-queue-system/
├── docker-compose.yml     # Boots postgres, redis, api, worker with one command
├── .env                   # Shared connection strings / tunables for api + worker
├── requirements.txt       # Single dependency list shared by both images
├── api/                   # FastAPI HTTP server
│   ├── main.py             # App wiring, lifespan, global error handlers
│   ├── models.py            # Pydantic request/response + payload schemas
│   ├── database.py          # All Postgres queries
│   ├── redis_client.py      # All Redis operations
│   └── routes/
│       ├── jobs.py           # POST /jobs, GET /jobs/{id}, GET /jobs
│       └── admin.py          # GET /admin/stats
├── worker/                # Background worker process (separate container)
│   ├── worker.py            # Main loop: BRPOP -> execute -> update
│   ├── utils.py              # DB/Redis helpers + logging used by the loop
│   └── job_handlers/
│       ├── email_handler.py  # Mock email sending
│       ├── csv_handler.py    # Real CSV de-duplication (pandas)
│       └── image_handler.py  # Real image resizing (Pillow)
├── migrations/
│   └── 001_init.sql       # Postgres schema (jobs, dead_letter_queue)
├── scripts/
│   └── seed_test_data.py  # Generates sample data + submits test jobs
└── tests/
    ├── test_api.py         # Payload validation + integration tests
    └── test_worker.py      # Job handler unit tests
```

## How a job's status changes over time

```
pending  →  running  →  completed
   ↑                        │
   └──── retry (≤3x) ───────┘
                             │
                        (retries exhausted)
                             ▼
                          failed  (+ row in dead_letter_queue)
```

- **Idempotency**: before executing any job, the worker checks whether
  that `job_id` is already `running` or `completed` in Postgres, and skips
  it if so — a job is never executed twice.
- **Retries**: on failure, the worker waits with exponential backoff
  (`2^attempt` seconds + random jitter) and re-queues the job, up to
  `MAX_RETRIES` (default 3, set in `.env`).
- **Dead letter queue**: once retries are exhausted, the job is marked
  `failed` and a full record — including every error message from every
  attempt — is written to the `dead_letter_queue` table for manual review.

## Running tests

```bash
pip install -r requirements.txt

# Fast, no external services needed — payload validation + job handler tests
pytest

# Full end-to-end tests against a live stack (run docker-compose up first)
pytest -m integration
```

## Configuration

All configuration lives in `.env`, loaded identically by both the `api`
and `worker` containers via docker-compose's `env_file`:

| Variable        | Purpose                                              |
|------------------|-------------------------------------------------------|
| `DATABASE_URL`   | Postgres connection string                            |
| `REDIS_URL`      | Redis connection string                                |
| `API_HOST`/`API_PORT` | Uvicorn bind address                              |
| `MAX_RETRIES`    | Retry attempts before a job is sent to the DLQ         |
| `WORKER_TIMEOUT` | Seconds the worker's `BRPOP` blocks before looping     |

## Phase 1 scope

Intentionally out of scope for this phase (see later phases):
- Multiple concurrent workers (Phase 2)
- Rate limiting (Phase 2)
- WebSocket live status updates (Phase 3)
- React frontend (Phase 3)

Phase 1's goal is a correct, observable, single-worker pipeline: submit →
queue → execute → persist, with retries and a dead letter queue for
permanent failures.
