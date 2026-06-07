import json
import logging
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

import pandas as pd


# ============================================================
# CONFIGURATION
# ============================================================

DATABASE_FILE = "database/weather.db"
SQL_FILE = "sql/create_tables.sql"

PROCESSED_FOLDER = "data/processed"

LOAD_METADATA_FOLDER = "data/load_metadata"
LOAD_METADATA_FILE = f"{LOAD_METADATA_FOLDER}/loaded_files.json"

LOG_DIR = "logs"
LOG_FILE = f"{LOG_DIR}/load.log"

EXPECTED_COLUMNS: set[str] = {
    # Define the columns your CSV files must contain.
    # Example: {"city", "temperature", "humidity", "timestamp"}
    # Leave empty set() to skip column validation.
}


# ============================================================
# LOGGING
# ============================================================

def setup_logging() -> logging.Logger:
    """
    Configure structured logging to both a daily log file and stdout.
    Console shows INFO+; file captures DEBUG+ for full traceability.
    """
    Path(LOG_DIR).mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger = logging.getLogger("weather_load")
    logger.setLevel(logging.DEBUG)

    # Avoid duplicate handlers if called multiple times
    if logger.handlers:
        return logger

    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


logger = setup_logging()


# ============================================================
# DATABASE
# ============================================================

@contextmanager
def get_connection() -> Generator[sqlite3.Connection, None, None]:
    """
    Context manager that yields an open SQLite connection and guarantees
    it is closed on exit — even if an exception is raised.

    Isolation is handled manually: call conn.commit() / conn.rollback()
    explicitly so each unit of work is an atomic transaction.
    """
    Path("database").mkdir(parents=True, exist_ok=True)
    conn: sqlite3.Connection | None = None

    try:
        conn = sqlite3.connect(DATABASE_FILE, isolation_level=None)  # autocommit off
        conn.execute("PRAGMA journal_mode=WAL;")   # better concurrency
        conn.execute("PRAGMA foreign_keys=ON;")
        logger.debug("SQLite connection opened: %s", DATABASE_FILE)
        yield conn
    except sqlite3.Error as e:
        logger.critical("Failed to open database connection: %s", e)
        raise
    finally:
        if conn:
            conn.close()
            logger.debug("SQLite connection closed.")


def create_tables(conn: sqlite3.Connection) -> None:
    """
    Run the DDL script to create tables if they don't yet exist.

    Raises:
        FileNotFoundError: if the SQL file is missing.
        sqlite3.Error: if the script execution fails.
    """
    sql_path = Path(SQL_FILE)

    if not sql_path.exists():
        raise FileNotFoundError(
            f"SQL file not found: {sql_path.resolve()}. "
            "Ensure 'sql/create_tables.sql' exists before running."
        )

    try:
        sql_script = sql_path.read_text(encoding="utf-8")
        conn.executescript(sql_script)
        conn.commit()
        logger.info("Database tables verified/created.")
    except sqlite3.Error as e:
        raise sqlite3.Error(f"Failed to execute DDL script '{SQL_FILE}': {e}") from e


# ============================================================
# LOAD METADATA
# ============================================================

def load_processed_files() -> list[str]:
    """
    Return the list of filenames already loaded into the database.
    Returns an empty list if the metadata file doesn't exist yet.

    Raises:
        json.JSONDecodeError: if the metadata file is corrupted.
        OSError: if the file exists but cannot be read.
    """
    Path(LOAD_METADATA_FOLDER).mkdir(parents=True, exist_ok=True)
    meta_path = Path(LOAD_METADATA_FILE)

    if not meta_path.exists():
        logger.debug("No load metadata file found — starting fresh.")
        return []

    try:
        with meta_path.open("r", encoding="utf-8") as f:
            metadata = json.load(f)
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(
            f"Load metadata file is corrupted ('{LOAD_METADATA_FILE}'): {e.msg}",
            e.doc,
            e.pos,
        ) from e
    except OSError as e:
        raise OSError(f"Cannot read load metadata file: {e}") from e

    loaded = metadata.get("loaded_files", [])

    if not isinstance(loaded, list):
        logger.warning(
            "Metadata 'loaded_files' is not a list — resetting to empty. "
            "Original value: %s", loaded
        )
        return []

    return loaded


def save_processed_files(processed_files: list[str]) -> None:
    """
    Persist the list of successfully loaded filenames to disk.

    Raises:
        OSError: if the file cannot be written.
    """
    metadata = {
        "loaded_files": processed_files,
        "last_updated": datetime.now(tz=timezone.utc).isoformat(),
    }

    try:
        with open(LOAD_METADATA_FILE, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=4)
        logger.debug("Load metadata saved: %d file(s) recorded.", len(processed_files))
    except OSError as e:
        raise OSError(f"Failed to save load metadata to '{LOAD_METADATA_FILE}': {e}") from e


# ============================================================
# FILE DISCOVERY
# ============================================================

def get_new_csv_files() -> list[Path]:
    """
    Discover CSV files in PROCESSED_FOLDER that have not been loaded yet.

    Raises:
        FileNotFoundError: if PROCESSED_FOLDER does not exist.
    """
    processed_path = Path(PROCESSED_FOLDER)

    if not processed_path.exists():
        raise FileNotFoundError(
            f"Processed folder not found: '{processed_path.resolve()}'. "
            "Run the transformation step before loading."
        )

    loaded_files = load_processed_files()
    loaded_set = set(loaded_files)  # O(1) lookups

    csv_files = list(processed_path.glob("*.csv"))
    logger.debug("Total CSV files found in folder: %d", len(csv_files))

    new_files = sorted(f for f in csv_files if f.name not in loaded_set)
    logger.info("New CSV files to load: %d", len(new_files))

    return new_files


# ============================================================
# CSV VALIDATION
# ============================================================

def validate_dataframe(df: pd.DataFrame, filename: str) -> None:
    """
    Run basic sanity checks on a freshly loaded DataFrame.

    Raises:
        ValueError: if the file fails validation.
    """
    if df.empty:
        raise ValueError(f"File '{filename}' is empty — no rows to load.")

    if EXPECTED_COLUMNS:
        actual_columns = set(df.columns)
        missing = EXPECTED_COLUMNS - actual_columns
        if missing:
            raise ValueError(
                f"File '{filename}' is missing required columns: {missing}. "
                f"Found: {actual_columns}"
            )

    null_counts = df.isnull().sum()
    columns_with_nulls = null_counts[null_counts > 0]
    if not columns_with_nulls.empty:
        logger.warning(
            "File '%s' contains null values — %s",
            filename,
            columns_with_nulls.to_dict(),
        )


# ============================================================
# LOAD CSV INTO DATABASE
# ============================================================

def load_csv_to_database(
    conn: sqlite3.Connection,
    csv_file: Path,
) -> int:
    """
    Read a CSV file, validate it, and append its rows to
    weather_observations.

    Returns:
        Number of rows loaded.

    Raises:
        ValueError: if validation fails.
        pd.errors.ParserError: if CSV parsing fails.
        sqlite3.Error: if DB insert fails.
    """

    logger.info("Loading file: %s", csv_file.name)

    # ── Read CSV ────────────────────────────────────────────────
    try:
        df = pd.read_csv(csv_file, encoding="utf-8")

    except pd.errors.EmptyDataError:
        raise ValueError(
            f"File '{csv_file.name}' is empty."
        )

    except pd.errors.ParserError as e:
        raise pd.errors.ParserError(
            f"Could not parse '{csv_file.name}': {e}"
        ) from e

    except OSError as e:
        raise OSError(
            f"Cannot read file '{csv_file}': {e}"
        ) from e

    # ── Validate DataFrame ─────────────────────────────────────
    validate_dataframe(df, csv_file.name)

    rows = len(df)

    # ── Load into SQLite ───────────────────────────────────────
    try:

        df.to_sql(
            "weather_observations",
            conn,
            if_exists="append",
            index=False,
            method="multi"
        )

    except Exception as e:

        logger.exception(
            "Database insert failed for %s",
            csv_file.name
        )

        raise sqlite3.Error(
            f"DB insert failed for '{csv_file.name}': {e}"
        ) from e

    logger.info(
        "Loaded %d rows from '%s'.",
        rows,
        csv_file.name
    )

    return rows


# ============================================================
# AUDIT TABLE
# ============================================================

def insert_audit_record(
    conn: sqlite3.Connection,
    source_file: str,
    rows_loaded: int,
    status: str,
    error_message: str | None = None,
) -> None:
    """
    Write one row to etl_load_audit.

    This runs in its own transaction so an audit write doesn't interfere
    with (or get rolled back by) the data-load transaction.

    Raises:
        sqlite3.Error: if the audit insert itself fails (logged as CRITICAL).
    """
    try:
        conn.execute("BEGIN")
        conn.execute(
            """
            INSERT INTO etl_load_audit
                (source_file, rows_loaded, load_status, error_message)
            VALUES
                (?, ?, ?, ?)
            """,
            (source_file, rows_loaded, status, error_message),
        )
        conn.execute("COMMIT")
        logger.debug("Audit record written: file=%s status=%s", source_file, status)
    except sqlite3.Error as e:
        conn.execute("ROLLBACK")
        # Audit failure must not suppress the main pipeline error — log only.
        logger.critical(
            "CRITICAL: Failed to write audit record for '%s': %s", source_file, e
        )


# ============================================================
# MAIN
# ============================================================

def main() -> int:
    """
    Entry point.

    Exit codes:
        0 — all files loaded successfully
        1 — one or more files failed (partial load)
        2 — fatal setup error (DB/config/folder missing), nothing loaded
    """
    run_start = datetime.now(tz=timezone.utc)
    logger.info("========== Load Process Started | %s ==========", run_start.isoformat())

    # ── Database setup ────────────────────────────────────────────────────────
    try:
        with get_connection() as conn:
            try:
                create_tables(conn)
            except (FileNotFoundError, sqlite3.Error) as e:
                logger.critical("Table setup failed — aborting: %s", e)
                return 2

            # ── File discovery ────────────────────────────────────────────────
            try:
                new_files = get_new_csv_files()
            except (FileNotFoundError, json.JSONDecodeError, OSError) as e:
                logger.critical("File discovery failed — aborting: %s", e)
                return 2

            if not new_files:
                logger.info("No new CSV files found. Nothing to load.")
                return 0

            # ── Process each file ─────────────────────────────────────────────
            loaded_files = load_processed_files()
            total_rows = 0
            failed_files: list[str] = []

            for csv_file in new_files:
                try:
                    rows = load_csv_to_database(conn, csv_file)
                    insert_audit_record(conn, csv_file.name, rows, "SUCCESS")
                    loaded_files.append(csv_file.name)
                    total_rows += rows

                except (ValueError, OSError, sqlite3.Error, pd.errors.ParserError) as e:
                    error_msg = str(e)
                    logger.error("[FAILED] %s — %s", csv_file.name, error_msg)
                    insert_audit_record(conn, csv_file.name, 0, "FAILED", error_msg)
                    failed_files.append(csv_file.name)

            # ── Persist metadata ──────────────────────────────────────────────
            try:
                save_processed_files(loaded_files)
            except OSError as e:
                # Data is already in DB — this is recoverable but must be flagged.
                logger.error(
                    "Data loaded but metadata save failed. "
                    "Re-runs may reload already-processed files. Error: %s", e
                )

    except sqlite3.Error as e:
        logger.critical("Unrecoverable database error: %s", e)
        return 2

    # ── Summary ───────────────────────────────────────────────────────────────
    duration = (datetime.now(tz=timezone.utc) - run_start).total_seconds()
    success_count = len(new_files) - len(failed_files)

    logger.info("========== Load Summary ==========")
    logger.info("Files discovered  : %d", len(new_files))
    logger.info("Files loaded      : %d", success_count)
    logger.info("Files failed      : %d", len(failed_files))
    logger.info("Total rows loaded : %d", total_rows)
    logger.info("Duration          : %.2fs", duration)

    if failed_files:
        logger.warning("Failed files: %s", ", ".join(failed_files))
        logger.info("==================================")
        return 1

    logger.info("==================================")
    return 0


if __name__ == "__main__":
    sys.exit(main())