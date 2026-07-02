"""
scripts/seed_test_data.py
------------------
PURPOSE: A one-shot script for manually exercising the running system.
         Generates sample input files, submits a mix of all three job
         types through the real HTTP API, and prints the resulting job
         IDs so you can immediately poll GET /jobs/{id} and watch each one
         move from pending -> running -> completed.

HOW TO RUN:
  1. Start the full stack:    docker-compose up
  2. From the repo root (on your HOST machine, not inside a container),
     run:                     python scripts/seed_test_data.py

WHY THIS RUNS ON THE HOST (not inside a container): it writes sample files
into ./uploads, which docker-compose bind-mounts into both the api and
worker containers at /app/uploads (see docker-compose.yml). Writing here
on the host is the same as writing directly into the containers' shared
volume — no extra step needed to get the files "into" Docker.

WHY IT TALKS TO localhost:8000 (not the "api" service hostname): Docker's
internal service-name DNS ("api", "postgres", "redis") only resolves
INSIDE the Docker network, between containers. This script runs on the
host, so it must use the port docker-compose published to the host
(8000:8000 — see docker-compose.yml), exactly like a browser or curl
would.
"""

import random
import sys

import httpx
import pandas as pd
from PIL import Image

API_BASE_URL = "http://localhost:8000"
UPLOADS_DIR = "uploads"


def generate_sample_csv(path: str, total_rows: int = 500) -> None:
    """
    Generate a CSV with `total_rows` rows, some of which are EXACT
    duplicates of one another — so the process_csv job has real
    deduplication work to do and non-trivial stats to report.

    HOW DUPLICATES ARE CREATED: we build a pool of unique people smaller
    than total_rows, then sample from that pool WITH replacement. Any
    person who gets picked more than once produces an exact-duplicate row
    (identical across every column), which is exactly what pandas'
    duplicated() checks for in worker/job_handlers/csv_handler.py.
    """
    first_names = ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy"]
    last_names = ["Smith", "Jones", "Lee", "Brown", "Garcia", "Kim", "Patel", "Nguyen"]

    # A pool of ~60 unique people — deliberately smaller than
    # total_rows=500, so sampling with replacement is guaranteed to
    # produce a meaningful number of exact duplicate rows.
    pool = [
        {
            "name": f"{first} {last}",
            "email": f"{first.lower()}.{last.lower()}@example.com",
            "age": random.randint(18, 65),
        }
        for first in first_names
        for last in last_names[:6]
    ]

    rows = [random.choice(pool) for _ in range(total_rows)]
    df = pd.DataFrame(rows)
    df.to_csv(path, index=False)
    print(f"Generated {path} ({total_rows} rows, {len(pool)}-person pool -> guaranteed duplicates)")


def generate_sample_image(path: str) -> None:
    """
    Generate a simple solid-color placeholder image so the resize_image
    job has a real file to open and resize — no need to source/commit a
    real photo just for a smoke test.
    """
    Image.new("RGB", (1920, 1080), color=(70, 130, 180)).save(path)
    print(f"Generated {path} (1920x1080)")


def submit_job(client: httpx.Client, user_id: str, job_type: str, payload: dict) -> str:
    """
    POST one job to the API and return its job_id.
    Raises if the API rejects the submission (e.g. validation failure) —
    a seed script should fail loudly, not silently skip a job.
    """
    response = client.post(
        f"{API_BASE_URL}/jobs",
        json={"user_id": user_id, "type": job_type, "payload": payload},
    )
    response.raise_for_status()
    body = response.json()
    return body["job_id"]


def main() -> None:
    import os

    os.makedirs(UPLOADS_DIR, exist_ok=True)

    csv_path = os.path.join(UPLOADS_DIR, "sample.csv")
    image_path = os.path.join(UPLOADS_DIR, "sample.jpg")

    generate_sample_csv(csv_path, total_rows=500)
    generate_sample_image(image_path)

    # Confirm the API is actually reachable before submitting jobs, so a
    # forgotten `docker-compose up` produces one clear error instead of
    # five confusing connection-refused tracebacks.
    try:
        httpx.get(f"{API_BASE_URL}/", timeout=3).raise_for_status()
    except httpx.HTTPError as exc:
        print(f"ERROR: could not reach the API at {API_BASE_URL} — is `docker-compose up` running?")
        print(f"  ({exc})")
        sys.exit(1)

    # A mix of all three job types, matching the "5 jobs of mixed types"
    # requirement: 2 CSV jobs, 2 email jobs, 1 image-resize job.
    jobs_to_submit = [
        ("process_csv", {"file_path": csv_path}),
        ("process_csv", {"file_path": csv_path}),
        ("send_email", {"to": "alice@example.com", "subject": "Welcome!", "body": "Thanks for signing up."}),
        ("send_email", {"to": "bob@example.com", "subject": "Reminder", "body": "Your invoice is due soon."}),
        (
            "resize_image",
            {"image_path": image_path, "sizes": [[800, 600], [400, 300], [100, 100]]},
        ),
    ]

    print(f"\nSubmitting {len(jobs_to_submit)} jobs to {API_BASE_URL} ...\n")

    with httpx.Client() as client:
        for job_type, payload in jobs_to_submit:
            job_id = submit_job(client, user_id="seed-script", job_type=job_type, payload=payload)
            print(f"  [{job_type:>13}] job_id = {job_id}")

    print(
        "\nDone. Track these jobs with:\n"
        f"  GET {API_BASE_URL}/jobs/<job_id>\n"
        f"  GET {API_BASE_URL}/jobs?user_id=seed-script\n"
        f"  GET {API_BASE_URL}/admin/stats\n"
    )


if __name__ == "__main__":
    main()
