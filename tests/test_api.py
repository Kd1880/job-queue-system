"""
tests/test_api.py
------------------
PURPOSE: Tests for the FastAPI HTTP/validation layer.

TWO KINDS OF TESTS IN THIS FILE:

1. Payload/request VALIDATION tests (the majority) — pure Pydantic model
   tests with no network or database involved. These run instantly, every
   time, with no setup, and cover the "did the API reject bad input"
   contract described in api/models.py.

2. A small set of END-TO-END integration tests marked `@pytest.mark.integration`
   that hit a REAL running api service over HTTP (i.e. you must have
   already run `docker-compose up`). These are skipped by default (see
   pytest.ini) because they depend on external services being up — run
   them explicitly with `pytest -m integration` after starting the stack.
   WHY BOTHER WITH BOTH: the validation tests catch regressions fast and
   in any environment; the integration tests are the only way to actually
   prove the full pipeline (API -> Postgres -> Redis -> back to API) is
   wired together correctly, which is the whole point of Phase 1.
"""

import httpx
import pytest
from pydantic import ValidationError

from api.models import CsvPayload, EmailPayload, ImagePayload, JobSubmitRequest


# ============================================================================
# PER-TYPE PAYLOAD SCHEMA TESTS
# ============================================================================

class TestEmailPayload:
    def test_accepts_valid_payload(self):
        payload = EmailPayload(to="user@example.com", subject="Hi", body="Hello there")
        assert payload.to == "user@example.com"

    def test_rejects_invalid_email_address(self):
        # EmailStr should reject anything that isn't real email syntax —
        # catching typos before a job ever reaches the queue.
        with pytest.raises(ValidationError):
            EmailPayload(to="not-an-email", subject="Hi", body="Hello there")

    def test_rejects_empty_subject(self):
        with pytest.raises(ValidationError):
            EmailPayload(to="user@example.com", subject="", body="Hello there")


class TestCsvPayload:
    def test_accepts_valid_payload(self):
        payload = CsvPayload(file_path="uploads/data.csv")
        assert payload.file_path == "uploads/data.csv"

    def test_rejects_missing_file_path(self):
        with pytest.raises(ValidationError):
            CsvPayload()


class TestImagePayload:
    def test_accepts_valid_payload(self):
        payload = ImagePayload(image_path="uploads/photo.jpg", sizes=[[800, 600], [100, 100]])
        assert payload.sizes == [(800, 600), (100, 100)]

    def test_rejects_empty_sizes_list(self):
        with pytest.raises(ValidationError):
            ImagePayload(image_path="uploads/photo.jpg", sizes=[])

    def test_rejects_non_positive_dimensions(self):
        # (0, 600) and (-100, 100) are both nonsensical resize targets —
        # see the sizes_must_be_positive validator in api/models.py.
        with pytest.raises(ValidationError):
            ImagePayload(image_path="uploads/photo.jpg", sizes=[[0, 600]])


# ============================================================================
# JobSubmitRequest CROSS-FIELD VALIDATION TESTS
# These exercise the model_validator that checks `payload` matches the
# schema implied by `type` — the core "validate payload fields per job
# type" requirement from the API spec.
# ============================================================================

class TestJobSubmitRequest:
    def test_accepts_matching_payload_for_send_email(self):
        request = JobSubmitRequest(
            user_id="user-123",
            type="send_email",
            payload={"to": "user@example.com", "subject": "Hi", "body": "Hello"},
        )
        assert request.type.value == "send_email"

    def test_accepts_matching_payload_for_process_csv(self):
        request = JobSubmitRequest(
            user_id="user-123",
            type="process_csv",
            payload={"file_path": "uploads/data.csv"},
        )
        assert request.payload["file_path"] == "uploads/data.csv"

    def test_rejects_payload_missing_required_field_for_type(self):
        # type=send_email but payload has no "to" — this is exactly the
        # bug class this cross-field validator exists to catch before the
        # job ever reaches the worker.
        with pytest.raises(ValidationError):
            JobSubmitRequest(
                user_id="user-123",
                type="send_email",
                payload={"subject": "Hi", "body": "Hello"},
            )

    def test_rejects_unknown_job_type(self):
        with pytest.raises(ValidationError):
            JobSubmitRequest(
                user_id="user-123",
                type="delete_universe",
                payload={},
            )


# ============================================================================
# END-TO-END INTEGRATION TESTS (require `docker-compose up` running)
# ============================================================================

BASE_URL = "http://localhost:8000"


@pytest.mark.integration
class TestJobsEndpointsIntegration:
    """
    Exercises the full HTTP -> Postgres -> Redis pipeline against a live
    stack. Run with: `docker-compose up -d && pytest -m integration`
    """

    def test_submit_job_returns_pending_instantly(self):
        response = httpx.post(
            f"{BASE_URL}/jobs",
            json={
                "user_id": "test-user",
                "type": "send_email",
                "payload": {"to": "test@example.com", "subject": "Test", "body": "Hello"},
            },
            timeout=5,
        )
        assert response.status_code == 201
        body = response.json()
        assert body["status"] == "pending"
        assert "job_id" in body

    def test_submit_then_fetch_job_status(self):
        submit_response = httpx.post(
            f"{BASE_URL}/jobs",
            json={
                "user_id": "test-user",
                "type": "send_email",
                "payload": {"to": "test@example.com", "subject": "Test", "body": "Hello"},
            },
            timeout=5,
        )
        job_id = submit_response.json()["job_id"]

        get_response = httpx.get(f"{BASE_URL}/jobs/{job_id}", timeout=5)
        assert get_response.status_code == 200
        body = get_response.json()
        assert body["job_id"] == job_id
        assert body["status"] in ("pending", "running", "completed")

    def test_get_nonexistent_job_returns_404(self):
        response = httpx.get(
            f"{BASE_URL}/jobs/00000000-0000-0000-0000-000000000000", timeout=5
        )
        assert response.status_code == 404
        assert "error" in response.json()

    def test_submit_invalid_job_type_returns_422(self):
        response = httpx.post(
            f"{BASE_URL}/jobs",
            json={"user_id": "test-user", "type": "not_a_real_type", "payload": {}},
            timeout=5,
        )
        assert response.status_code == 422
        assert "error" in response.json()

    def test_admin_stats_returns_expected_shape(self):
        response = httpx.get(f"{BASE_URL}/admin/stats", timeout=5)
        assert response.status_code == 200
        body = response.json()
        assert "queue_depth" in body
        assert "jobs_by_status" in body
        assert "dlq_count" in body
        assert "jobs_last_hour" in body
