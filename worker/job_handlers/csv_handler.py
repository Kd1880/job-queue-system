"""
worker/job_handlers/csv_handler.py
------------------
PURPOSE: Executes a `process_csv` job — reads a CSV file, de-duplicates
         it, and writes a cleaned copy to disk.

HOW IT FITS IN THE SYSTEM:
  Called by worker/worker.py's execute_job() dispatcher whenever a job's
  `type` is "process_csv". This is a REAL (not mocked) implementation:
  pandas actually reads and transforms the file on the shared uploads/
  volume that both the api and worker containers mount (see
  docker-compose.yml). Deliberately synchronous (`def`, not `async def`) —
  pandas has no async API, and this is CPU/disk-bound work, not I/O-bound
  waiting on a network call. worker/worker.py runs this via
  `asyncio.to_thread` so it doesn't block the event loop while it runs.
"""

import os

import pandas as pd


def handle_process_csv(payload: dict) -> dict:
    """
    Read a CSV, remove duplicate rows, and save the cleaned result.

    ARGS:
      payload: {"file_path": str} — path to the input CSV, relative to the
               shared working directory (e.g. "uploads/data.csv"). Already
               validated for presence/type by CsvPayload (api/models.py);
               whether the FILE ITSELF actually exists is checked here,
               at execution time, since it could theoretically be uploaded
               moments after the job was submitted.

    FLOW:
      1. Verify the file exists — raise a clear error if not, so the
         worker's retry/DLQ logic (worker/worker.py) has a meaningful
         error_message to record instead of a raw pandas traceback.
      2. Read the CSV into a DataFrame.
      3. Count total rows before any cleaning.
      4. Count duplicate rows: a row counts as a duplicate if EVERY column
         value matches another row exactly (pandas' default
         duplicated() behavior).
      5. Drop duplicates, keeping the first occurrence of each.
      6. Write the cleaned DataFrame to processed/cleaned_{filename}.
      7. Return stats describing what was done.

    RETURNS:
      {
        "original_rows": int,
        "duplicate_rows": int,
        "cleaned_rows": int,
        "columns": [str, ...],
        "output_file": str,
      }
    """
    file_path = payload["file_path"]

    # Step 1: Fail fast with a clear message if the input file is missing,
    # rather than letting pandas raise its own (less friendly) FileNotFoundError
    # deep inside read_csv.
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    # Step 2: Read the CSV into memory. For Phase 1's scale (portfolio-
    # sized files, not multi-GB datasets) an in-memory read is simple and
    # fast; a streaming/chunked read would be the Phase 2+ approach for
    # very large files.
    df = pd.read_csv(file_path)

    # Step 3: Row count before any cleaning — needed to report how many
    # duplicates were removed.
    original_rows = len(df)

    # Step 4: duplicated() returns a boolean Series, True for every row
    # that is an exact repeat of an earlier row (checked across ALL
    # columns by default). Summing the booleans counts them (True == 1).
    duplicate_rows = int(df.duplicated().sum())

    # Step 5: Drop duplicate rows, keeping the first occurrence of each
    # distinct row. This is the actual "cleaning" the job promises.
    cleaned_df = df.drop_duplicates()
    cleaned_rows = len(cleaned_df)

    # Step 6: Write the cleaned file to processed/cleaned_{original_filename}.
    # os.makedirs(..., exist_ok=True) ensures the processed/ directory
    # exists even on a totally clean checkout — we don't want this job to
    # fail just because nobody manually created the output folder first.
    filename = os.path.basename(file_path)
    output_dir = "processed"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"cleaned_{filename}")

    # index=False: don't write pandas' internal row-number index as an
    # extra CSV column — the caller only wants the original columns back.
    cleaned_df.to_csv(output_file, index=False)

    # Step 7: Return stats. columns as a plain list (not a pandas Index)
    # so it serializes cleanly to JSON when stored in Postgres's `result`
    # JSONB column.
    return {
        "original_rows": original_rows,
        "duplicate_rows": duplicate_rows,
        "cleaned_rows": cleaned_rows,
        "columns": list(df.columns),
        "output_file": output_file,
    }
