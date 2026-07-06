"""
api/middleware/
------------------
PURPOSE: Request-level cross-cutting concerns that run around route
         handlers — things that apply to "requests in general" rather
         than any one endpoint's business logic. Phase 2 adds the first
         one: rate limiting (rate_limiter.py). Auth would live here in a
         future phase for the same reason.
"""
