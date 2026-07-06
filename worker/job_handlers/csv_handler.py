"""
worker/job_handlers/csv_handler.py
------------------
PURPOSE: Two data job handlers:
  - handle_process_csv   (Phase 1, kept unchanged): simple de-duplication.
  - handle_data_pipeline (Phase 2): a full ETL pipeline — validate,
    clean, transform, compute stats and a 0-100 data quality score.

HOW IT FITS IN THE SYSTEM:
  Called by worker/worker.py's execute_job() dispatcher for job types
  "process_csv" / "data_pipeline". Deliberately synchronous (`def`, not
  `async def`) — pandas has no async API, and this is CPU/disk-bound
  work. worker/worker.py runs both via `asyncio.to_thread`.

ETL IN 30 SECONDS:
  Extract  — get raw data out of wherever it lives (here: a CSV/JSON file
             on the shared uploads/ volume).
  Transform — validate, clean, and reshape it into trustworthy, consistent
             data (the bulk of this file).
  Load     — write the result somewhere downstream consumers can use it
             (here: processed/ on the shared volume).
  Every data-engineering stack (Airflow, dbt, Spark jobs) is this same
  three-beat pattern at bigger scale.

VALIDATION vs CLEANING (they are different jobs):
  Validation ANSWERS "is this data acceptable?" — checks that fail fast
  and reject the whole file (e.g. a required column is missing entirely:
  no amount of per-row fixing recovers from that).
  Cleaning FIXES what's fixable row by row — trims whitespace, lowercases
  emails, drops duplicates/nulls — and REPORTS what it fixed, so the
  quality score reflects how dirty the input was.
"""

import os
import re

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


# ============================================================================
# PHASE 2: FULL ETL PIPELINE (job type "data_pipeline")
# ============================================================================

# Pragmatic email shape check: something@something.tld. Deliberately NOT
# the full RFC 5322 grammar (which famously allows quoted spaces and other
# horrors nobody's signup form accepts) — for data-quality reporting, a
# simple pattern that matches 99.9% of real addresses beats a "perfect"
# 6KB regex that's impossible to review.
_EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")


def _read_dataframe(file_path: str) -> pd.DataFrame:
    """
    EXTRACT stage: load the raw file into a DataFrame.

    Supports .csv and .json by extension. Encoding: try UTF-8 first (the
    modern default), fall back to latin-1 on decode failure — latin-1 maps
    every possible byte to SOME character, so the read always succeeds;
    worst case a few accented characters look odd, which the cleaning
    stage can survive. (A production system would use charset sniffing —
    e.g. the `charset-normalizer` library — but two-step fallback covers
    the overwhelmingly common cases without a new dependency.)
    """
    if file_path.lower().endswith(".json"):
        return pd.read_json(file_path)

    try:
        return pd.read_csv(file_path, encoding="utf-8")
    except UnicodeDecodeError:
        return pd.read_csv(file_path, encoding="latin-1")


def handle_data_pipeline(payload: dict) -> dict:
    """
    Run the full ETL pipeline over one uploaded file.

    ARGS:
      payload: validated by DataPipelinePayload (api/models.py):
        {"file_path": str, "operations": [...],
         "expected_columns": [...] | None, "drop_duplicates": bool,
         "handle_nulls": "drop"|"fill", "output_format": "csv"|"json"}

    RETURNS (the shape stored in Postgres's result JSONB):
      {"original_rows", "cleaned_rows", "removed_rows",
       "issues_found": {duplicates, null_values, invalid_emails},
       "column_stats": {col: {...}}, "output_file", "quality_score"}
    """
    file_path = payload["file_path"]
    operations = payload["operations"]

    # Fail fast with a clear message for the retry/DLQ error history.
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input file not found: {file_path}")

    # ---- EXTRACT ----
    df = _read_dataframe(file_path)
    original_rows = len(df)

    # Normalize column names once, before anything else touches them:
    # "  Name " and "name" should be the same column. Doing this FIRST
    # means expected_columns validation and every stage below can rely on
    # clean, lowercase names.
    df.columns = [str(col).strip().lower() for col in df.columns]

    # ---- VALIDATE ----
    # Validation failures raise (whole-file rejection): a missing required
    # column is not fixable row-by-row, and silently proceeding would
    # produce a "cleaned" file missing the data the caller cares about.
    # The raised ValueError flows into worker.py's retry/DLQ machinery —
    # and since the file won't grow a column by retrying, it will land in
    # the DLQ with this exact message for a human to act on.
    if "validate" in operations and payload.get("expected_columns"):
        expected = [col.strip().lower() for col in payload["expected_columns"]]
        missing = [col for col in expected if col not in df.columns]
        if missing:
            raise ValueError(
                f"Validation failed: expected columns missing {missing}; "
                f"file has {list(df.columns)}"
            )

    # ------------------------------------------------------------------
    # MEASURE ISSUES ON THE RAW DATA (before cleaning mutates it).
    # The quality score must describe the data AS RECEIVED — measuring
    # after cleaning would always report a perfect file.
    # ------------------------------------------------------------------

    # Rows that are an exact copy of an earlier row, across all columns.
    duplicate_count = int(df.duplicated().sum())

    # Total null CELLS (not rows): a row missing both age and city counts
    # 2 here — a truer measure of how much data is absent.
    null_count = int(df.isnull().sum().sum())

    # Rows containing at least one null — used for both the drop stage
    # and the quality score's "% complete rows" component.
    rows_with_nulls = int(df.isnull().any(axis=1).sum())

    # Invalid emails: measured after trimming/lowercasing (a padded but
    # otherwise fine address shouldn't count as broken — cleaning will fix
    # it), against non-null values only (missing emails are already
    # counted in null_count; counting them twice would double-punish).
    invalid_email_count = 0
    valid_email_ratio = 1.0  # neutral when there's no email column at all
    if "email" in df.columns:
        emails = df["email"].dropna().astype(str).str.strip().str.lower()
        if len(emails) > 0:
            is_valid = emails.str.match(_EMAIL_RE)
            invalid_email_count = int((~is_valid).sum())
            valid_email_ratio = float(is_valid.mean())

    # ---- CLEAN ----
    if "clean" in operations:
        # Strip leading/trailing whitespace from every string column.
        # " Delhi" and "Delhi" must be ONE city, or unique_values/
        # most_common in the stats stage report nonsense.
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].str.strip()

        # Emails are case-insensitive by spec (RFC 5321 treats the domain
        # so, and every real provider treats the whole address so) —
        # lowercase them or "A@x.com" and "a@x.com" dodge the duplicate
        # check below.
        if "email" in df.columns:
            df["email"] = df["email"].str.lower()

        # Names: keep letters, spaces, dots, hyphens, apostrophes (real
        # names contain all of those — "O'Brien", "Jean-Luc") and drop
        # the rest (stray digits, emoji, markup fragments from scraping).
        if "name" in df.columns:
            df["name"] = df["name"].str.replace(r"[^A-Za-z\s.\-']", "", regex=True).str.strip()

        # Duplicates AFTER normalization: " a@x.com" vs "a@x.com" rows
        # only become detectable duplicates once whitespace/case are fixed.
        if payload.get("drop_duplicates", True):
            df = df.drop_duplicates()

        # Null policy comes from the payload — same handler, two policies:
        #   drop: strict — a row missing any field is untrustworthy, remove
        #   fill: lenient — keep every row; numeric nulls get the column
        #         mean (preserves the column's average), text nulls get "".
        if payload.get("handle_nulls", "drop") == "drop":
            df = df.dropna()
        else:
            for col in df.columns:
                if pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].fillna(df[col].mean())
                else:
                    df[col] = df[col].fillna("")

    # ---- TRANSFORM ----
    if "transform" in operations:
        # Type casting: CSV has no types — EVERYTHING arrives as text, so
        # a numeric column with one "N/A" cell parses as strings ("32"
        # not 32), breaking min/max/mean. Re-attempt numeric conversion
        # per text column: if ALL its (non-null) values convert cleanly,
        # adopt the numeric dtype; if ANY fail, keep it as text —
        # errors="coerce" turns failures into NaN, so we only adopt when
        # no new NaNs appeared (never silently destroying real values).
        for col in df.select_dtypes(include=["object"]).columns:
            converted = pd.to_numeric(df[col], errors="coerce")
            if converted.notna().sum() == df[col].notna().sum():
                df[col] = converted

    # ---- STATS ----
    column_stats: dict = {}
    if "stats" in operations:
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]) and df[col].notna().any():
                column_stats[col] = {
                    "min": round(float(df[col].min()), 2),
                    "max": round(float(df[col].max()), 2),
                    "mean": round(float(df[col].mean()), 2),
                }
            elif df[col].notna().any():
                # value_counts() sorts by frequency descending, so
                # .index[0] is the most common value — the categorical
                # equivalent of a mean.
                column_stats[col] = {
                    "unique_values": int(df[col].nunique()),
                    "most_common": str(df[col].value_counts().index[0]),
                }

    # ---- LOAD ----
    os.makedirs("processed", exist_ok=True)
    base_name, _ = os.path.splitext(os.path.basename(file_path))
    output_format = payload.get("output_format", "csv")
    output_file = os.path.join("processed", f"cleaned_{base_name}.{output_format}")

    if output_format == "json":
        # orient="records": [{row}, {row}, ...] — the shape every API
        # consumer expects, not pandas' default column-oriented dict.
        df.to_json(output_file, orient="records", indent=2)
    else:
        # index=False: don't leak pandas' internal row numbers as a column.
        df.to_csv(output_file, index=False)

    # ---- QUALITY SCORE ----
    # One 0-100 number summarizing how clean the input was — the metric a
    # dashboard tracks over time ("supplier X's files degraded this
    # month"). Equal-weight average of three independent health checks,
    # all measured on the RAW input:
    #   completeness — % of rows with no missing values
    #   validity     — % of well-formed emails
    #   uniqueness   — % of non-duplicate rows
    if original_rows > 0:
        completeness = 1 - (rows_with_nulls / original_rows)
        uniqueness = 1 - (duplicate_count / original_rows)
        quality_score = round((completeness + valid_email_ratio + uniqueness) / 3 * 100, 1)
    else:
        quality_score = 0.0

    cleaned_rows = len(df)
    return {
        "original_rows": original_rows,
        "cleaned_rows": cleaned_rows,
        "removed_rows": original_rows - cleaned_rows,
        "issues_found": {
            "duplicates": duplicate_count,
            "null_values": null_count,
            "invalid_emails": invalid_email_count,
        },
        "column_stats": column_stats,
        "output_file": output_file,
        "quality_score": quality_score,
    }
