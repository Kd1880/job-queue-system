"""
api/redis_client.py
------------------
PURPOSE: Owns every direct interaction between the FastAPI app and Redis.
         In Phase 1, Redis has exactly one job: act as the handoff queue
         between "a job was submitted" (api) and "a worker should execute
         it" (worker), via a single Redis LIST named "jobs:queue".

HOW IT FITS IN THE SYSTEM:
  Postgres (api/database.py) is permanent storage — every job's full
  history lives there forever. Redis is deliberately NOT permanent storage:
  it's an in-memory, microsecond-latency inbox. A job sits in the Redis
  list only from the moment it's submitted until a worker's BRPOP picks it
  up — usually milliseconds to seconds. If Redis restarted and lost the
  list entirely, the jobs table in Postgres would still show those jobs as
  'pending' forever (a known Phase 1 limitation — a reconciliation sweep
  that re-enqueues stale 'pending' rows is a natural Phase 2 addition).

WHY A REDIS LIST (not a Redis Stream or Pub/Sub):
  A plain LIST + LPUSH/BRPOP gives us exactly the FIFO queue semantics we
  need with the simplest possible data structure: one queue, one consumer
  (for now), no consumer groups or acknowledgement complexity. Streams
  would be the right choice once Phase 2 introduces multiple competing
  workers that need at-least-once delivery guarantees with explicit ACKs.
"""

import os

import redis.asyncio as redis

# The single Redis key every job is pushed to and popped from. Named with a
# "jobs:" prefix as a light namespacing convention — if this system grows
# additional Redis-backed features later (e.g. a "jobs:dlq_notify" pubsub
# channel), keys stay easy to tell apart with `redis-cli KEYS "jobs:*"`.
QUEUE_KEY = "jobs:queue"

# Same pattern as DATABASE_URL in api/database.py: read once from the
# environment injected by docker-compose's env_file (.env), shared
# identically by the api and worker containers.
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")


def create_redis_client() -> redis.Redis:
    """
    Create the async Redis client used for the lifetime of the app.

    redis.asyncio (the official async client bundled in redis-py 5.x)
    manages its own internal connection pool, so — unlike asyncpg, where we
    explicitly configure pool size — a single client instance is already
    safe to share across every concurrent FastAPI request handler.

    decode_responses=True means every value we get back from Redis is a
    Python str (not raw bytes), so callers don't need to manually
    .decode('utf-8') the job JSON on every read.
    """
    return redis.from_url(REDIS_URL, decode_responses=True)


async def close_redis_client(client: redis.Redis) -> None:
    """Close the underlying connection pool on app shutdown."""
    await client.aclose()


async def push_job(client: redis.Redis, job_json: str) -> None:
    """
    Push a newly submitted job onto the queue.

    LPUSH = Left Push -> adds `job_json` to the LEFT end of the Redis list.
    The worker consumes with BRPOP (Right Pop, see worker/utils.py), which
    reads from the RIGHT end. Pairing LPUSH (producer) with RPOP (consumer)
    on the same list gives strict FIFO order: the first job LPUSHed is the
    first job BRPOPed, i.e. "first submitted, first processed".

    WHAT IF THIS FAILS: the caller (api/routes/jobs.py) has already
    committed the Postgres row with status='pending' before calling this.
    If LPUSH raises (e.g. Redis is briefly unreachable), that row is now
    stuck at 'pending' with no corresponding queue entry — visible via
    GET /jobs/{id} as a job that never progresses, which is the safer
    failure mode compared to a phantom queue entry with no DB record.
    """
    await client.lpush(QUEUE_KEY, job_json)


async def get_queue_depth(client: redis.Redis) -> int:
    """
    Report how many jobs are currently waiting in the queue (not yet
    popped by a worker). Used by GET /admin/stats.

    LLEN = List Length -> O(1) in Redis (the list tracks its own length,
    no need to walk every element), so calling this on every stats request
    is cheap even as the queue grows.
    """
    return await client.llen(QUEUE_KEY)
