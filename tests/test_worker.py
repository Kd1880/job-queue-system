"""
tests/test_worker.py
------------------
PURPOSE: Tests for the job handlers — the actual work each job type
         performs (send_email / process_csv / resize_image) — independent
         of the queue/database plumbing around them.

WHY NO POSTGRES/REDIS HERE: worker/worker.py's main loop is thin
orchestration glue (BRPOP -> call a handler -> write status) that is best
proven correct by the integration tests in tests/test_api.py (which
exercise the full pipeline end-to-end through a live stack). This file
instead unit-tests the handlers directly with real files on disk — no
mocking needed, since pandas/Pillow/asyncio.sleep are all fast and
deterministic for the small inputs used here.
"""

import os

import pandas as pd
import pytest
from PIL import Image

from worker.job_handlers.csv_handler import handle_process_csv
from worker.job_handlers.email_handler import handle_send_email
from worker.job_handlers.image_handler import handle_resize_image


# ============================================================================
# CSV HANDLER TESTS
# ============================================================================

class TestCsvHandler:
    def test_removes_duplicate_rows_and_reports_stats(self, tmp_path, monkeypatch):
        # Run inside a temp directory so the handler's relative
        # "processed/" output path doesn't pollute the real repo.
        monkeypatch.chdir(tmp_path)

        # Build a small CSV with 2 exact-duplicate rows out of 5.
        df = pd.DataFrame(
            {
                "name": ["Alice", "Bob", "Alice", "Carol", "Bob"],
                "email": ["a@x.com", "b@x.com", "a@x.com", "c@x.com", "b@x.com"],
            }
        )
        input_path = tmp_path / "sample.csv"
        df.to_csv(input_path, index=False)

        result = handle_process_csv({"file_path": str(input_path)})

        assert result["original_rows"] == 5
        assert result["duplicate_rows"] == 2
        assert result["cleaned_rows"] == 3
        assert result["columns"] == ["name", "email"]
        assert os.path.exists(result["output_file"])

        # The cleaned file on disk should actually have the deduplicated
        # row count, not just the value reported in the returned stats.
        cleaned = pd.read_csv(result["output_file"])
        assert len(cleaned) == 3

    def test_missing_file_raises_file_not_found(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        with pytest.raises(FileNotFoundError):
            handle_process_csv({"file_path": "does/not/exist.csv"})


# ============================================================================
# IMAGE HANDLER TESTS
# ============================================================================

class TestImageHandler:
    def test_creates_one_resized_copy_per_requested_size(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        # A tiny real image is enough to exercise Pillow's actual resize
        # logic — no need for a large fixture file.
        source_path = tmp_path / "photo.png"
        Image.new("RGB", (1920, 1080), color="blue").save(source_path)

        sizes = [[800, 600], [400, 300], [100, 100]]
        result = handle_resize_image({"image_path": str(source_path), "sizes": sizes})

        assert result["original_size"] == [1920, 1080]
        assert len(result["output_files"]) == 3

        for entry in result["output_files"]:
            assert os.path.exists(entry["path"])
            # thumbnail() preserves aspect ratio, so the saved file's
            # actual dimensions fit WITHIN the requested box but won't
            # necessarily match it exactly — just confirm it's no larger.
            with Image.open(entry["path"]) as saved:
                assert saved.size[0] <= entry["size"][0]
                assert saved.size[1] <= entry["size"][1]

    def test_missing_file_raises_file_not_found(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        with pytest.raises(FileNotFoundError):
            handle_resize_image({"image_path": "does/not/exist.jpg", "sizes": [[100, 100]]})


# ============================================================================
# EMAIL HANDLER TESTS
# ============================================================================

class TestEmailHandler:
    async def test_returns_expected_result_shape(self, monkeypatch):
        # Patch asyncio.sleep so this test doesn't actually wait 1 real
        # second — we're testing the handler's output shape, not timing.
        import worker.job_handlers.email_handler as email_handler_module

        async def instant_sleep(_seconds):
            return None

        monkeypatch.setattr(email_handler_module.asyncio, "sleep", instant_sleep)

        result = await handle_send_email(
            {"to": "someone@example.com", "subject": "Hi", "body": "Hello"}
        )

        assert result["to"] == "someone@example.com"
        assert result["message_id"].startswith("mock-")
        assert "sent_at" in result
