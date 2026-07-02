-- migrations/001_init.sql
-- ------------------
-- PURPOSE: Defines the permanent Postgres schema for the job queue system.
--          Postgres is the SOURCE OF TRUTH for job state — Redis only ever
--          holds a transient, in-flight copy of a job (the queue). If Redis
--          crashes and loses data, Postgres still has the full history.
--
-- HOW IT'S LOADED: docker-compose mounts this file into
--   /docker-entrypoint-initdb.d/001_init.sql inside the postgres container.
--   The official postgres image automatically executes every .sql file in
--   that directory, in filename order, but ONLY the first time the data
--   volume is created (i.e. on a fresh `docker-compose up` with no existing
--   volume). This is why the filename is prefixed "001_" — future schema
--   changes would be "002_....sql" and run in order.

-- pgcrypto provides gen_random_uuid(), which we use as the default for
-- every primary key. UUIDs (instead of auto-incrementing integers) mean job
-- IDs are globally unique and unguessable without ever needing to ask the
-- database "what ID did you just assign me?" — useful since the API
-- generates job IDs before the row is even inserted in some designs, and
-- guarantees no collision if we ever run multiple Postgres instances.
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================================
-- TABLE: jobs
-- The single table tracking every job ever submitted, across its entire
-- lifecycle: pending -> running -> completed | failed.
-- ============================================================================
CREATE TABLE jobs (
    -- Primary key. Generated server-side by Postgres by default, but in
    -- practice the API generates the UUID itself (see api/routes/jobs.py)
    -- so the SAME id can be used in the Redis payload and the Postgres row
    -- from the very first write — no round trip needed to learn the id.
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Who submitted the job. Not a foreign key to a users table because
    -- Phase 1 has no auth/user system yet — it's a free-text identifier
    -- used purely for filtering ("show me all of user X's jobs").
    user_id         VARCHAR(100) NOT NULL,

    -- Job type: 'send_email' | 'process_csv' | 'resize_image'.
    -- Kept as VARCHAR rather than a Postgres ENUM so adding new job types
    -- in the future never requires a schema migration (ALTER TYPE is
    -- notoriously painful in Postgres) — validation instead happens in the
    -- Pydantic layer (api/models.py).
    type            VARCHAR(50) NOT NULL,

    -- Arbitrary job-specific input, e.g. {"file_path": "uploads/data.csv"}.
    -- JSONB (binary JSON) instead of JSON because JSONB is stored in a
    -- parsed binary format — faster to query/index later, and de-duplicates
    -- whitespace, at the small cost of slightly slower writes (irrelevant
    -- here since jobs are written once).
    payload         JSONB NOT NULL,

    -- Current lifecycle state. Defaults to 'pending' because the row is
    -- always inserted BEFORE the job is pushed to Redis (see comment in
    -- api/routes/jobs.py explaining write-order).
    status          VARCHAR(20) NOT NULL DEFAULT 'pending',

    -- Output of a successfully completed job (handler-specific shape).
    -- NULL until the worker finishes; JSONB for the same reasons as payload.
    result          JSONB,

    -- Human-readable error message from the most recent failed attempt.
    -- Kept even after a successful retry would be nice for debugging, but
    -- Phase 1 simply overwrites/clears it as needed — see
    -- worker/utils.py update functions.
    error_message   TEXT,

    -- How many times this job has been retried after failure. Starts at 0.
    -- Compared against MAX_RETRIES in the worker to decide retry vs DLQ.
    retry_count     INTEGER DEFAULT 0,

    -- Lifecycle timestamps. created_at is set once at INSERT time;
    -- started_at/completed_at are NULL until the worker reaches that stage.
    -- Having all three lets us compute queue wait time (started_at -
    -- created_at) and execution time (completed_at - started_at) for free,
    -- which is exactly what GET /admin/stats and job introspection need.
    created_at      TIMESTAMP DEFAULT NOW(),
    started_at      TIMESTAMP,
    completed_at    TIMESTAMP
);

-- Index on status: GET /admin/stats runs "SELECT status, COUNT(*) ... GROUP
-- BY status" on every call, and the worker's idempotency check filters by
-- status too. Without this index, both would require a full table scan
-- (O(n)) once the jobs table grows past a few thousand rows.
CREATE INDEX idx_jobs_status ON jobs(status);

-- Index on user_id: GET /jobs?user_id=... is the most common read query
-- from the API (every "list my jobs" call). Without this index, Postgres
-- would scan every row in the table to find one user's jobs.
CREATE INDEX idx_jobs_user_id ON jobs(user_id);

-- Index on created_at, descending: job listings are always returned
-- "newest first" (see GET /jobs ORDER BY created_at DESC). A descending
-- index lets Postgres satisfy that ORDER BY directly from the index without
-- an extra sort step.
CREATE INDEX idx_jobs_created_at ON jobs(created_at DESC);

-- Composite index on (user_id, status): GET /jobs?user_id=X&status=Y is a
-- very common combined filter. A composite index here lets Postgres
-- satisfy both predicates from a single index lookup instead of
-- intersecting two separate index scans (idx_jobs_user_id +
-- idx_jobs_status), which is significantly faster at scale.
CREATE INDEX idx_jobs_user_status ON jobs(user_id, status);

-- ============================================================================
-- TABLE: dead_letter_queue
-- Jobs that failed MAX_RETRIES times land here instead of being silently
-- dropped. This gives operators a place to inspect, manually fix, and
-- re-submit permanently-broken jobs — a standard pattern in distributed
-- queue systems (equivalent to AWS SQS's DLQ concept).
-- ============================================================================
CREATE TABLE dead_letter_queue (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- References the id of the row in `jobs` that ultimately failed.
    -- Deliberately NOT a foreign key: we want the DLQ record to survive
    -- even if the original jobs row were ever purged/archived, so this
    -- table is a fully independent audit record, not a dependent one.
    original_job_id UUID NOT NULL,

    job_type        VARCHAR(50),

    -- Snapshot of the payload at time of final failure, so a human can
    -- inspect exactly what input caused the failure without needing to
    -- join back to the (mutable) jobs table.
    original_payload JSONB,

    -- Full history of every error message across all retry attempts
    -- (not just the last one) — a TEXT[] array lets us see the whole
    -- failure story, e.g. ["Connection refused", "Connection refused",
    -- "File not found"], which is often more diagnostic than the final
    -- error alone.
    error_log       TEXT[],

    failed_at       TIMESTAMP DEFAULT NOW(),

    -- Simple workflow state for a human reviewing the DLQ:
    -- 'pending_review' -> (manually) 'resolved' / 'ignored' / etc.
    -- Phase 1 doesn't build UI for this yet, but the column exists so
    -- Phase 2/3 admin tooling can use it without another migration.
    status          VARCHAR(20) DEFAULT 'pending_review'
);
