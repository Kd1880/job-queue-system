"""
PURPOSE: Per-user rate limiting for job submission — max N jobs (default
         5) per user per minute. Protects the queue from a single
         misbehaving/buggy client flooding it with thousands of jobs and
         starving every other user (the "noisy neighbor" problem).

HOW IT FITS IN THE SYSTEM:
  Attached to POST /jobs as a FastAPI dependency (see api/routes/jobs.py).
  It runs BEFORE the route handler — an over-limit request is rejected
  with HTTP 429 before a job row is inserted or anything touches the
  queue. Costs one Redis INCR (~sub-millisecond) per request.

THE PATTERN — FIXED-WINDOW COUNTER via Redis INCR + EXPIRE:
  Key:   rate_limit:{user_id}:{current_minute}   e.g.
         rate_limit:user-123:2026-07-05T15:44
  
HTTP 429 "Too Many Requests": the standard status for rate limiting.
  We include a Retry-After header + retry_after body field so a
  well-behaved client knows exactly how long to back off instead of
  guessing (or worse, hammering harder).
"""

import os
from datetime import datetime, timezone

from fastapi import Request

# Env-tunable so ops can loosen/tighten limits without a code deploy.
RATE_LIMIT_PER_MINUTE = int(os.environ.get("RATE_LIMIT_PER_MINUTE", "5"))


class RateLimitExceeded(Exception):
   

    def __init__(self, limit: int, retry_after: int):
        self.limit = limit
        self.retry_after = retry_after
        super().__init__(f"Rate limit exceeded: {limit}/minute")


async def check_rate_limit(request: Request) -> None:

    try:
        body = await request.json()
    except Exception:
        return
    if not isinstance(body, dict):
        return
    user_id = body.get("user_id")
    if not user_id or not isinstance(user_id, str):
        return

   
    now = datetime.now(timezone.utc)
    window = now.strftime("%Y-%m-%dT%H:%M")
    key = f"rate_limit:{user_id}:{window}"

    redis_client = request.app.state.redis_client

    count = await redis_client.incr(key)

    if count == 1:
       
        await redis_client.expire(key, 60)

    if count > RATE_LIMIT_PER_MINUTE:
       
        retry_after = max(60 - now.second, 1)
        raise RateLimitExceeded(limit=RATE_LIMIT_PER_MINUTE, retry_after=retry_after)
