"""
load_weather.py
===============
Stage 3 of the Weather Analytics ETL pipeline.

Responsibility: read processed CSVs produced by transform.py and insert
their rows into the SQLite weather database.

Designed to be called by master_pipeline.py → run_load(), but also
runnable standalone for debugging (python load_weather.py).

Public API (consumed by master_pipeline.py)
-------------------------------------------
  get_connection()                                  → contextmanager[sqlite3.Connection]
  create_tables(conn)                               → None
  load_processed_files()                            → list[str]
  save_processed_files(processed_files)             → None
  get_new_csv_files()                               → list[Path]
  validate_dataframe(df, filename)                  → None   (raises ValueError on failure)
  load_csv_to_database(conn, csv_file)              → int    (rows loaded)
  insert_audit_record(conn, source_file, rows, ...) → None
"""

from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

import pandas as pd

# ── Shared definitions ────────────────────────────────────────────────────────
from pipeline_config import DEFAULTS, get_logger, load_config


# ─────────────────────────────────────────────────────────────────────────────
# NOTE: No module-level logger here.
#       Each function accepts a logger argument (when called from the pipeline)
#       or calls _get_load_logger() (when running standalone).
#       This guarantees that importing this module never causes side effects.
# ─────────────────────────────────────────────────────────────────────────────

def _get_load_logger():
    """Return the named load logger. Call only inside functions, never at module level."""
    return get_logger(
        name     = "weather_load",
        log_file = f"weather_load_{datetime.now().strftime('%Y%m%d')}.log",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Schema  — columns the CSVs must contain (empty = skip validation)
# ─────────────────────────────────────────────────────────────────────────────

EXPECTED_COLUMNS: set[str] = set()


# ─────────────────────────────────────────────────────────────────────────────
# Database Connection
# ─────────────────────────────────────────────────────────────────────────────

@contextmanager
def get_connection(
    database_file: str = DEFAULTS.database_file,
) -> Generator[sqlite3.Connection, None, None]:
    """
    Context manager yielding an open SQLite connection.

    Isolation is manual: call conn.commit() / conn.rollback() explicitly
    so each unit of work is an atomic transaction.
    """
    logger = _get_load_logger()
    Path(database_file).parent.mkdir(parents=True, exist_ok=True)
    conn: sqlite3.Connection | None = None

    try:
        conn = sqlite3.connect(database_file, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        logger.debug("SQLite connection opened: %s", database_file)
        yield conn
    except sqlite3.Error as e:
        logger.critical("Failed to open database connection: %s", e)
        raise
    finally:
        if conn:
            conn.close()
            logger.debug("SQLite connection closed.")


def create_tables(
    conn:     sqlite3.Connection,
    sql_file: str = DEFAULTS.sql_file,
) -> None:
    """
    Run the DDL script to create tables if they don't yet exist.

    Raises:
        FileNotFoundError — SQL file missing
        sqlite3.Error     — script execution failed
    """
    logger   = _get_load_logger()
    sql_path = Path(sql_file)

    if not sql_path.exists():
        raise FileNotFoundError(
            f"SQL file not found: {sql_path.resolve()}. "
            "Ensure 'sql/create_tables.sql' exists before running."
        )

    try:
        conn.executescript(sql_path.read_text(encoding="utf-8"))
        conn.commit()
        logger.info("Database tables verified/created.")
    except sqlite3.Error as e:
        raise sqlite3.Error(f"Failed to execute DDL script '{sql_file}': {e}") from e


# ─────────────────────────────────────────────────────────────────────────────
# Load Metadata  — tracks which CSVs have already been loaded into the DB
# ─────────────────────────────────────────────────────────────────────────────

def load_processed_files(
    metadata_file: str = DEFAULTS.load_metadata_file,
) -> list[str]:
    """
    Return filenames already loaded into the database.
    Returns [] if the metadata file does not exist yet.

    Raises:
        json.JSONDecodeError — metadata file is corrupted
        OSError              — file exists but cannot be read
    """
    logger    = _get_load_logger()
    meta_path = Path(metadata_file)
    meta_path.parent.mkdir(parents=True, exist_ok=True)

    if not meta_path.exists():
        logger.debug("No load metadata file found — starting fresh.")
        return []

    try:
        with meta_path.open("r", encoding="utf-8") as f:
            metadata = json.load(f)
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(
            f"Load metadata file is corrupted ('{metadata_file}'): {e.msg}",
            e.doc, e.pos,
        ) from e
    except OSError as e:
        raise OSError(f"Cannot read load metadata file: {e}") from e

    loaded = metadata.get("loaded_files", [])
    if not isinstance(loaded, list):
        logger.warning("Metadata 'loaded_files' is not a list — resetting to empty.")
        return []

    return loaded


def save_processed_files(
    processed_files: list[str],
    metadata_file:   str = DEFAULTS.load_metadata_file,
) -> None:
    """
    Persist the list of successfully loaded filenames.

    Raises:
        OSError — file cannot be written
    """
    logger = _get_load_logger()
    metadata = {
        "loaded_files": processed_files,
        "last_updated": datetime.now(tz=timezone.utc).isoformat(),
    }
    try:
        with open(metadata_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=4)
        logger.debug("Load metadata saved: %d file(s) recorded.", len(processed_files))
    except OSError as e:
        raise OSError(f"Failed to save load metadata to '{metadata_file}': {e}") from e


# ─────────────────────────────────────────────────────────────────────────────
# File Discovery
# ─────────────────────────────────────────────────────────────────────────────

def get_new_csv_files(
    processed_folder: str = DEFAULTS.processed_folder,
    metadata_file:    str = DEFAULTS.load_metadata_file,
) -> list[Path]:
    """
    Discover CSVs in processed_folder that have not yet been loaded.

    Raises:
        FileNotFoundError — processed_folder does not exist
    """
    logger         = _get_load_logger()
    processed_path = Path(processed_folder)

    if not processed_path.exists():
        raise FileNotFoundError(
            f"Processed folder not found: '{processed_path.resolve()}'. "
            "Run the transformation step before loading."
        )

    loaded_set = set(load_processed_files(metadata_file))
    csv_files  = list(processed_path.glob("*.csv"))
    logger.debug("Total CSV files in folder: %d", len(csv_files))

    new_files = sorted(f for f in csv_files if f.name not in loaded_set)
    logger.info("New CSV files to load: %d", len(new_files))
    return new_files


# ─────────────────────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────────────────────

def validate_dataframe(df: pd.DataFrame, filename: str) -> None:
    """
    Generic pre-load validation.

    Transform stage owns schema.
    Load stage only validates data quality.
    """

    logger = _get_load_logger()

    # Empty dataframe
    if df.empty:
        raise ValueError(
            f"File '{filename}' is empty — no rows to load."
        )

    # Duplicate rows
    duplicate_count = df.duplicated().sum()

    if duplicate_count > 0:
        raise ValueError(
            f"Found {duplicate_count} duplicate rows."
        )

    # Entirely null columns
    empty_columns = [
        col
        for col in df.columns
        if df[col].isnull().all()
    ]

    if empty_columns:
        raise ValueError(
            f"Columns contain only NULL values: {empty_columns}"
        )

    # Log partial nulls
    null_counts = df.isnull().sum()
    cols_with_nulls = null_counts[null_counts > 0]

    if not cols_with_nulls.empty:
        logger.warning(
            "File '%s' contains null values: %s",
            filename,
            cols_with_nulls.to_dict(),
        )

    logger.info(
        "Validation passed: %d rows, %d columns.",
        len(df),
        len(df.columns),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Load CSV → Database
# ─────────────────────────────────────────────────────────────────────────────

def load_csv_to_database(
    conn:     sqlite3.Connection,
    csv_file: Path,
) -> int:
    """
    Read a CSV file, validate it, and append rows to weather_observations.

    Returns:
        Number of rows loaded.

    Raises:
        ValueError            — validation failure or empty file
        pd.errors.ParserError — CSV parsing failed
        sqlite3.Error         — DB insert failed
        OSError               — file cannot be read
    """
    logger = _get_load_logger()
    logger.info("Loading file: %s", csv_file.name)

    try:
        df = pd.read_csv(csv_file, encoding="utf-8")
    except pd.errors.EmptyDataError:
        raise ValueError(f"File '{csv_file.name}' is empty.")
    except pd.errors.ParserError as e:
        raise pd.errors.ParserError(f"Could not parse '{csv_file.name}': {e}") from e
    except OSError as e:
        raise OSError(f"Cannot read file '{csv_file}': {e}") from e

    validate_dataframe(df, csv_file.name)
    rows = len(df)

    try:
        df.to_sql(
            "weather_observations",
            conn,
            if_exists = "append",
            index     = False,
            method    = "multi",
        )
    except Exception as e:
        logger.exception("Database insert failed for %s", csv_file.name)
        raise sqlite3.Error(f"DB insert failed for '{csv_file.name}': {e}") from e

    logger.info("Loaded %d rows from '%s'.", rows, csv_file.name)
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Audit Table
# ─────────────────────────────────────────────────────────────────────────────

def insert_audit_record(
    conn:          sqlite3.Connection,
    source_file:   str,
    rows_loaded:   int,
    status:        str,
    error_message: str | None = None,
) -> None:
    """
    Write one row to etl_load_audit in its own transaction.

    An audit write failure does NOT suppress the main pipeline error — it is
    logged at CRITICAL level and the pipeline continues.
    """
    logger = _get_load_logger()
    try:
        conn.execute("BEGIN")
        conn.execute(
            """
            INSERT INTO etl_load_audit
                (source_file, rows_loaded, load_status, error_message)
            VALUES (?, ?, ?, ?)
            """,
            (source_file, rows_loaded, status, error_message),
        )
        conn.execute("COMMIT")
        logger.debug("Audit record: file=%s status=%s", source_file, status)
    except sqlite3.Error as e:
        conn.execute("ROLLBACK")
        logger.critical(
            "CRITICAL: Failed to write audit record for '%s': %s", source_file, e
        )


# ─────────────────────────────────────────────────────────────────────────────
# Standalone entry point
# Only used when running:  python load_weather.py
# master_pipeline.py calls the functions above directly via run_load().
# ─────────────────────────────────────────────────────────────────────────────

def _standalone_main() -> int:
    """
    Exit codes:
        0 — all files loaded successfully
        1 — partial failure (some files failed)
        2 — fatal setup error (DB/config/folder missing)
    """
    logger    = _get_load_logger()
    run_start = datetime.now(tz=timezone.utc)
    logger.info("===== Load Stage (standalone) | %s =====", run_start.isoformat())

    # Load config for any overridden paths
    try:
        config = load_config()
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
        logger.critical("Config error — aborting: %s", e)
        return 2

    database_file    = config.get("database_file",     DEFAULTS.database_file)
    sql_file         = config.get("sql_file",          DEFAULTS.sql_file)
    processed_folder = config.get("processed_folder",  DEFAULTS.processed_folder)
    metadata_file    = config.get("load_metadata_file", DEFAULTS.load_metadata_file)

    try:
        with get_connection(database_file) as conn:
            try:
                create_tables(conn, sql_file)
            except (FileNotFoundError, sqlite3.Error) as e:
                logger.critical("Table setup failed — aborting: %s", e)
                return 2

            try:
                new_files = get_new_csv_files(processed_folder, metadata_file)
            except (FileNotFoundError, json.JSONDecodeError, OSError) as e:
                logger.critical("File discovery failed — aborting: %s", e)
                return 2

            if not new_files:
                logger.info("No new CSV files found. Nothing to load.")
                return 0

            loaded_list:  list[str] = load_processed_files(metadata_file)
            total_rows:   int       = 0
            failed_files: list[str] = []

            for csv_file in new_files:
                try:
                    rows = load_csv_to_database(conn, csv_file)
                    insert_audit_record(conn, csv_file.name, rows, "SUCCESS")
                    loaded_list.append(csv_file.name)
                    total_rows += rows
                except (ValueError, OSError, sqlite3.Error, pd.errors.ParserError) as e:
                    msg = str(e)
                    logger.error("[FAILED] %s — %s", csv_file.name, msg)
                    insert_audit_record(conn, csv_file.name, 0, "FAILED", msg)
                    failed_files.append(csv_file.name)

            try:
                save_processed_files(loaded_list, metadata_file)
            except OSError as e:
                logger.error(
                    "Data loaded but metadata save failed. "
                    "Re-runs may reload already-processed files. Error: %s", e,
                )

    except sqlite3.Error as e:
        logger.critical("Unrecoverable database error: %s", e)
        return 2

    duration = (datetime.now(tz=timezone.utc) - run_start).total_seconds()
    success_count = len(new_files) - len(failed_files)

    logger.info("===== Load Summary =====")
    logger.info("Files discovered  : %d", len(new_files))
    logger.info("Files loaded      : %d", success_count)
    logger.info("Files failed      : %d", len(failed_files))
    logger.info("Total rows loaded : %d", total_rows)
    logger.info("Duration          : %.2fs", duration)

    if failed_files:
        logger.warning("Failed files: %s", ", ".join(failed_files))
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(_standalone_main())
