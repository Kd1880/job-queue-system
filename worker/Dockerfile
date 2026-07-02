# worker/Dockerfile
# ------------------
# PURPOSE: Builds the container image for the background worker process.
#
# Deliberately a SEPARATE image from api/Dockerfile (not one shared image
# running two different commands) so that api and worker can be scaled,
# deployed, and restarted independently — e.g. Phase 2 will run multiple
# worker replicas without needing multiple API replicas.
#
# Built with build context "." (repo root — see docker-compose.yml).

FROM python:3.11-slim

WORKDIR /app

# Same layer-caching trick as api/Dockerfile: copy requirements first so
# `pip install` is cached across rebuilds that only touch worker source.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy worker application source.
COPY worker/ ./worker

# Output directories written by the CSV/image handlers. See comment in
# api/Dockerfile — persisted via docker-compose bind mounts in practice.
RUN mkdir -p uploads processed resized

# No EXPOSE here — the worker is not an HTTP server, it never listens on a
# port. It only makes OUTBOUND connections to Redis and Postgres.

# Runs the worker as a module (`python -m worker.worker`) rather than a
# script (`python worker/worker.py`) so that the relative imports inside
# worker/ (e.g. `from worker.job_handlers import ...`) resolve correctly —
# module execution puts /app on sys.path as the package root.
CMD ["python", "-m", "worker.worker"]
