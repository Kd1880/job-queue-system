# api/Dockerfile
# ------------------
# PURPOSE: Builds the container image for the FastAPI HTTP server.
#
# Built with build context "." (the repo root — see docker-compose.yml),
# so COPY paths below are relative to the repo root, not to this file's
# directory. This lets us COPY the shared requirements.txt at the repo
# root without duplicating it into api/.

# Python 3.11 slim = official Python image stripped of build tools/docs we
# don't need. Much smaller than the default python:3.11 image, faster to
# pull, and every package we use (fastapi, asyncpg, pandas, Pillow) ships
# prebuilt wheels so we don't need a full compiler toolchain at runtime.
FROM python:3.11-slim

# All subsequent instructions (COPY, RUN, CMD) operate relative to /app
# inside the container. Keeps application code out of system directories
# like / or /usr, which is a Docker best practice.
WORKDIR /app

# Copy ONLY requirements.txt first, before the rest of the source code.
# WHY: Docker caches each layer. If requirements.txt hasn't changed since
# the last build, Docker reuses the cached "pip install" layer instead of
# re-downloading every package — this makes rebuilds after a code change
# (which happens constantly during development) take seconds instead of
# minutes.
COPY requirements.txt .

# --no-cache-dir: don't keep pip's download cache in the image layer —
# saves image size since we'll never run pip install again inside this
# container.
RUN pip install --no-cache-dir -r requirements.txt

# Now copy the actual application source. This layer changes on every code
# edit, but because it comes AFTER the pip install layer, the expensive
# dependency installation step is still cached.
COPY api/ ./api

# Directories the process-level job handlers/API write output files into.
# Created here so they exist even before the docker-compose bind mounts
# overlay them (the bind mount volumes in docker-compose.yml provide the
# real persisted versions on the host, but this keeps the image
# self-contained if run standalone without compose).
RUN mkdir -p uploads processed resized

# Document the port this container listens on. Purely informational for
# `docker inspect`/`docker ps` — the actual port mapping happens in
# docker-compose.yml's `ports:` section.
EXPOSE 8000

# Default command. docker-compose.yml overrides this with the --reload
# variant for local development, but this is what runs if the image is
# started standalone (e.g. `docker run jobqueue-api`), which matters for a
# production deployment where you would NOT want --reload (it costs
# performance and watches the filesystem unnecessarily).
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
