"""
master_pipeline.py
==================
Centralised orchestrator for the Weather Analytics ETL pipeline.

  Stage 1  EXTRACT   → OpenWeatherMap API  →  data/raw/*.json
  Stage 2  TRANSFORM → Flatten + enrich    →  data/processed/*.csv
  Stage 3  LOAD      → Insert into SQLite  →  database/weather.db

Data handoff (key design)
--------------------------
When stages are chained, each stage passes its output file list DIRECTLY
to the next stage — no folder-scanning or metadata lookup needed.
When a stage runs in ISOLATION (e.g. --stages transform), it falls back
to its own file-discovery logic (incremental, metadata-based).

All shared types (StageResult, load_config, get_logger, DEFAULTS) live
in pipeline_config.py. This file only orchestrates.

Usage
-----
  python master_pipeline.py                         # full pipeline (E→T→L)
  python master_pipeline.py --stages extract
  python master_pipeline.py --stages transform load
  python master_pipeline.py --stages all --fail-fast
  python master_pipeline.py --dry-run
  python master_pipeline.py --config path/to/cfg.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from export_powerbi_dataset import export_dataset

# ── Shared definitions ────────────────────────────────────────────────────────
from pipeline_config import (
    DEFAULTS,
    STAGE_ORDER,
    StageResult,
    get_logger,
    load_config,
)

# Prevent transform.py's old logging.basicConfig() (if any lingering copy is
# imported) from polluting our pipeline log output.
logging.getLogger().addHandler(logging.NullHandler())


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline logger
# ─────────────────────────────────────────────────────────────────────────────

def setup_pipeline_logger() -> logging.Logger:
    """Dedicated logger for the pipeline orchestrator."""
    return get_logger(
        name     = "master_pipeline",
        log_file = "pipeline.log",
        log_dir  = DEFAULTS.log_dir,
    )


# ─────────────────────────────────────────────────────────────────────────────
# STAGE RUNNERS
#
# Each runner:
#   • uses lazy imports (deferred inside the function) so module-level
#     side effects in transform / load_weather don't fire at startup
#   • accepts an optional `input_files` list:
#       - not None → explicit handoff from the previous stage (chained run)
#       - None     → standalone run, use own file-discovery logic
#   • returns a StageResult whose files_out feeds the next stage
# ─────────────────────────────────────────────────────────────────────────────

def run_extract(config: dict, logger: logging.Logger) -> StageResult:
    """
    Stage 1 — Fetch live weather data from OpenWeatherMap.

    Produces:  data/raw/weather_<timestamp>.json
    Returns:   StageResult.files_out = [path_to_raw_json]
    """
    from extract_weather import build_session, fetch_weather, save_output

    result = StageResult(
        stage      = "extract",
        status     = "RUNNING",
        started_at = datetime.now(tz=timezone.utc),
    )

    timestamp  = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    api_key    = config["api_key"].strip()
    cities     = [c.strip() for c in config["cities"]]
    units      = config.get("units",                   DEFAULTS.units)
    output_dir = config.get("output_dir",              DEFAULTS.raw_folder)
    delay      = float(config.get("request_delay_seconds", DEFAULTS.request_delay_seconds))

    session = build_session(
        retries        = config.get("http_retries",        DEFAULTS.http_retries),
        backoff_factor = config.get("http_backoff_factor", DEFAULTS.http_backoff_factor),
        timeout        = config.get("http_timeout_seconds", DEFAULTS.http_timeout_seconds),
    )

    successful_cities: list[dict] = []
    failed_cities:     list[dict] = []

    for city in cities:
        weather, err = fetch_weather(city, api_key, session, logger, units)
        if weather:
            successful_cities.append(weather)
        else:
            failed_cities.append(err)
        if delay > 0:
            time.sleep(delay)

    if not successful_cities:
        return result.complete(
            "FAILED",
            error = "All city requests failed — no data written.",
        )

    payload = {
        "ingestion_timestamp": timestamp,
        "total_requested":     len(cities),
        "successful_records":  len(successful_cities),
        "failed_records":      len(failed_cities),
        "units":               units,
        "weather_data":        successful_cities,
        "failed_cities":       failed_cities,
    }

    try:
        out_path = save_output(payload, output_dir, timestamp, logger)
    except OSError as e:
        return result.complete("FAILED", error=f"Could not write output file: {e}")

    logger.info(
        "Extract: %d/%d cities fetched → %s",
        len(successful_cities), len(cities), out_path,
    )

    return result.complete(
        "SUCCESS" if not failed_cities else "PARTIAL",
        files_out      = [out_path],
        rows_processed = len(successful_cities),
        error          = (
            f"Failed cities: {[fc.get('city') for fc in failed_cities]}"
            if failed_cities else None
        ),
    )


def run_transform(
    config:      dict,
    input_files: Optional[list[str]],   # None → standalone (scan folder)
    logger:      logging.Logger,
) -> StageResult:
    """
    Stage 2 — Flatten, clean, and enrich raw JSON file(s).

    input_files — explicit list of raw JSON paths produced by run_extract().
                  Pass None when transform runs in isolation (triggers
                  its own metadata-based file-discovery).

    Produces:   data/processed/<name>.csv  (one CSV per raw JSON)
    Returns:    StageResult.files_out = [csv_path, ...]
    """
    from transform import (
        load_metadata, get_unprocessed_files, mark_as_processed,
        load_raw_json, build_dataframe, clean_dataframe,
        add_derived_columns, add_business_columns, save_processed,
    )

    result = StageResult(
        stage      = "transform",
        status     = "RUNNING",
        started_at = datetime.now(tz=timezone.utc),
    )

    raw_folder       = config.get("raw_folder",       DEFAULTS.raw_folder)
    processed_folder = config.get("processed_folder", DEFAULTS.processed_folder)
    metadata_file    = config.get("metadata_file",    DEFAULTS.metadata_file)

    # Decide which files to process
    if input_files is not None:
        # CHAINED: process exactly the files extract just wrote
        filenames       = [Path(f).name for f in input_files]
        result.files_in = list(input_files)
        logger.info("Transform: received %d file(s) from extract.", len(filenames))
    else:
        # STANDALONE: discover any unprocessed raw files
        metadata        = load_metadata(metadata_file)
        filenames       = get_unprocessed_files(raw_folder, metadata)
        result.files_in = [str(Path(raw_folder) / fn) for fn in filenames]
        logger.info("Transform: discovered %d new raw file(s).", len(filenames))

    if not filenames:
        logger.info("Transform: no new files — nothing to do.")
        return result.complete("NO_OP")

    csv_out:    list[str] = []
    total_rows: int       = 0
    failed:     list[str] = []

    for filename in filenames:
        filepath = str(Path(raw_folder) / filename)
        try:
            wrapper = load_raw_json(filepath)
            df      = build_dataframe(wrapper)

            if df.empty:
                logger.warning("Transform: no records in %s — skipping.", filename)
                continue

            df = clean_dataframe(df)
            df = add_derived_columns(df)
            df = add_business_columns(df)

            csv_path = save_processed(df, filename, processed_folder)
            mark_as_processed(metadata_file, filename)

            csv_out.append(csv_path)
            total_rows += len(df)
            logger.info("Transform: %s → %s (%d rows)", filename, csv_path, len(df))

        except Exception as e:
            logger.error("Transform: failed on %s — %s", filename, e)
            failed.append(filename)

    status = (
        "SUCCESS" if not failed
        else "PARTIAL" if csv_out
        else "FAILED"
    )
    return result.complete(
        status,
        files_out      = csv_out,
        rows_processed = total_rows,
        error          = f"Failed files: {failed}" if failed else None,
    )


def run_load(
    input_files: Optional[list[str]],   # None → standalone (scan folder)
    logger:      logging.Logger,
) -> StageResult:
    """
    Stage 3 — Insert processed CSV(s) into the SQLite database.

    input_files — explicit list of CSV paths produced by run_transform().
                  Pass None when load runs in isolation (triggers its own
                  metadata-based file-discovery).

    Produces:   rows in database/weather.db::weather_observations
    Returns:    StageResult with rows_processed total
    """
    from load_weather import (
        get_connection, create_tables,
        load_csv_to_database, insert_audit_record,
        load_processed_files, save_processed_files,
        get_new_csv_files,
    )

    result = StageResult(
        stage      = "load",
        status     = "RUNNING",
        started_at = datetime.now(tz=timezone.utc),
    )

    if input_files is not None:
        # CHAINED: load exactly the CSVs transform just wrote
        csv_files       = [Path(f) for f in input_files]
        result.files_in = list(input_files)
        logger.info("Load: received %d file(s) from transform.", len(csv_files))
    else:
        # STANDALONE: discover any unloaded CSVs
        try:
            csv_files       = get_new_csv_files()
            result.files_in = [str(f) for f in csv_files]
        except FileNotFoundError as e:
            return result.complete("FAILED", error=str(e))
        logger.info("Load: discovered %d new CSV file(s).", len(csv_files))

    if not csv_files:
        logger.info("Load: no new files — nothing to do.")
        return result.complete("NO_OP")

    loaded_list:  list[str] = load_processed_files()
    total_rows:   int       = 0
    failed_files: list[str] = []

    try:
        with get_connection() as conn:
            create_tables(conn)   # idempotent — CREATE TABLE IF NOT EXISTS

            for csv_file in csv_files:
                try:
                    rows = load_csv_to_database(conn, csv_file)
                    insert_audit_record(conn, csv_file.name, rows, "SUCCESS")
                    loaded_list.append(csv_file.name)
                    total_rows += rows
                    logger.info("Load: %s → %d rows inserted.", csv_file.name, rows)

                except Exception as e:
                    msg = str(e)
                    logger.error("Load: [FAILED] %s — %s", csv_file.name, msg)
                    insert_audit_record(conn, csv_file.name, 0, "FAILED", msg)
                    failed_files.append(csv_file.name)

            try:
                save_processed_files(loaded_list)
            except OSError as e:
                logger.error(
                    "Load: data inserted but metadata save failed. "
                    "Re-runs may reload already-loaded files. Error: %s", e,
                )

    except sqlite3.Error as e:
        return result.complete("FAILED", error=f"Database error: {e}")

    status = (
        "SUCCESS" if not failed_files
        else "PARTIAL" if total_rows > 0
        else "FAILED"
    )
    return result.complete(
        status,
        files_out      = [str(f) for f in csv_files],
        rows_processed = total_rows,
        error          = f"Failed: {failed_files}" if failed_files else None,
    )


# ─────────────────────────────────────────────────────────────────────────────
# ORCHESTRATOR  — wires the three stage runners together
# ─────────────────────────────────────────────────────────────────────────────

def orchestrate(
    stages:    list[str],
    config:    dict,
    fail_fast: bool,
    dry_run:   bool,
    logger:    logging.Logger,
) -> list[StageResult]:
    """
    Run the requested stages in order, wiring outputs to inputs.

    Handoff chain:
        run_extract()
            └─ result.files_out  (raw JSON paths)
                 └─ run_transform(input_files=...)
                         └─ result.files_out  (CSV paths)
                              └─ run_load(input_files=...)

    Each stage only receives `input_files` when the previous stage
    ran in the same invocation. If a stage runs alone, input_files=None
    triggers that stage's own file-discovery / metadata logic.
    """
    results:   list[StageResult] = []
    raw_files: list[str]         = []   # extract  → transform
    csv_files: list[str]         = []   # transform → load

    run_e = "extract"   in stages
    run_t = "transform" in stages
    run_l = "load"      in stages

    # EXTRACT
    if run_e:
        _stage_header("EXTRACT", logger, dry_run)
        if not dry_run:
            res = run_extract(config, logger)
            results.append(res)
            _stage_footer(res, logger)
            raw_files = res.files_out

            if res.status == "FAILED" and fail_fast:
                logger.error("--fail-fast: pipeline stopped after EXTRACT failure.")
                return results

    # TRANSFORM
    if run_t:
        _stage_header("TRANSFORM", logger, dry_run)
        if not dry_run:
            res = run_transform(config, raw_files if run_e else None, logger)
            results.append(res)
            _stage_footer(res, logger)
            csv_files = res.files_out

            if res.status == "FAILED" and fail_fast:
                logger.error("--fail-fast: pipeline stopped after TRANSFORM failure.")
                return results

    # LOAD
    if run_l:
        _stage_header("LOAD", logger, dry_run)

        if not dry_run:
            res = run_load(csv_files if run_t else None, logger)
            results.append(res)
            _stage_footer(res, logger)

            # Export Power BI dataset only if load succeeded
            if res.status in ("SUCCESS", "PARTIAL"):

                logger.info("")
                logger.info("  ┌─────────────────────────────────────────┐")
                logger.info("  │  Stage: EXPORT POWER BI DATASET         │")
                logger.info("  └─────────────────────────────────────────┘")

                try:
                    csv_path = export_dataset()

                    logger.info(
                        "Power BI dataset exported: %s",
                        csv_path
                    )

                except Exception as e:
                    logger.error(
                        "Power BI export failed: %s",
                        e
                    )

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Display helpers
# ─────────────────────────────────────────────────────────────────────────────

_STATUS_LABEL = {
    "SUCCESS": "[OK]  ",
    "PARTIAL": "[WARN]",
    "FAILED":  "[FAIL]",
    "NO_OP":   "[SKIP]",
    "RUNNING": "[....]",
}


def _stage_header(name: str, logger: logging.Logger, dry_run: bool = False) -> None:
    prefix = "[DRY RUN] " if dry_run else ""
    logger.info("")
    logger.info("  ┌─────────────────────────────────────────┐")
    logger.info("  │  %sStage: %-33s│", prefix, name)
    logger.info("  └─────────────────────────────────────────┘")


def _stage_footer(result: StageResult, logger: logging.Logger) -> None:
    label = _STATUS_LABEL.get(result.status, "[?]")
    logger.info(
        "  %s  %-10s │ status=%-8s │ rows=%4d │ %.2fs",
        label, result.stage.upper(), result.status,
        result.rows_processed, result.duration_s,
    )
    if result.error:
        logger.warning("           ↳ %s", result.error)


def print_summary(
    results: list[StageResult],
    total_s: float,
    logger:  logging.Logger,
) -> None:
    """Print a formatted summary table after the pipeline finishes."""
    logger.info("")
    logger.info("  ╔═══════════════════════════════════════════════╗")
    logger.info("  ║           PIPELINE RUN SUMMARY                ║")
    logger.info("  ╠═══════════════════════════════════════════════╣")

    for r in results:
        label = _STATUS_LABEL.get(r.status, "[?]")
        logger.info(
            "  ║  %s  %-10s │ %-8s │ %5.2fs │ %4d rows  ║",
            label, r.stage.upper(), r.status, r.duration_s, r.rows_processed,
        )
        if r.error:
            err = r.error[:50] + "…" if len(r.error) > 50 else r.error
            logger.warning("  ║        ↳ %-37s  ║", err)

    logger.info("  ╠═══════════════════════════════════════════════╣")

    statuses = {r.status for r in results}
    if not results:
        overall = "NO_OP"
    elif "FAILED" in statuses and not any(r.status in ("SUCCESS", "PARTIAL") for r in results):
        overall = "FAILED"
    elif statuses <= {"SUCCESS", "NO_OP"}:
        overall = "SUCCESS"
    else:
        overall = "PARTIAL"

    label = _STATUS_LABEL.get(overall, "[?]")
    logger.info(
        "  ║  %s  Overall: %-8s        Total: %6.2fs      ║",
        label, overall, total_s,
    )
    logger.info("  ╚═══════════════════════════════════════════════╝")
    logger.info("")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog        = "master_pipeline.py",
        description = "Weather Analytics ETL Pipeline Orchestrator",
        formatter_class = argparse.RawDescriptionHelpFormatter,
        epilog = """
Stages
------
  extract    Fetch from OpenWeatherMap API  →  data/raw/*.json
  transform  Flatten and enrich raw JSON   →  data/processed/*.csv
  load       Insert CSVs into SQLite DB    →  database/weather.db

Examples
--------
  python master_pipeline.py
  python master_pipeline.py --stages extract
  python master_pipeline.py --stages transform load
  python master_pipeline.py --stages all --fail-fast
  python master_pipeline.py --dry-run

Exit codes
----------
  0  All stages succeeded (or had nothing to do)
  1  One or more stages partially succeeded
  2  Fatal error (config missing, database unreachable, etc.)
        """,
    )
    parser.add_argument(
        "--stages",
        nargs   = "+",
        choices = ["all", "extract", "transform", "load"],
        default = ["all"],
        metavar = "STAGE",
        help    = "Stages to run (default: all).",
    )
    parser.add_argument(
        "--config",
        default = DEFAULTS.config_file,
        metavar = "PATH",
        help    = f"Path to config.json (default: {DEFAULTS.config_file}).",
    )
    parser.add_argument(
        "--fail-fast",
        action = "store_true",
        help   = "Stop the pipeline on the first stage failure.",
    )
    parser.add_argument(
        "--dry-run",
        action = "store_true",
        help   = "Print which stages would run without executing anything.",
    )
    return parser.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    args   = parse_args()
    logger = setup_pipeline_logger()

    # Preserve STAGE_ORDER to guarantee E→T→L sequence
    if "all" in args.stages:
        stages = STAGE_ORDER
    else:
        stages = [s for s in STAGE_ORDER if s in args.stages]

    run_ts = datetime.now(tz=timezone.utc)
    logger.info("")
    logger.info("  ══════════════════════════════════════════════════════")
    logger.info("    WEATHER ANALYTICS PIPELINE")
    logger.info("    Started : %s", run_ts.strftime("%Y-%m-%d %H:%M:%S UTC"))
    logger.info("    Stages  : %s", stages)
    logger.info("    Flags   : fail-fast=%s  dry-run=%s", args.fail_fast, args.dry_run)
    logger.info("  ══════════════════════════════════════════════════════")

    if args.dry_run:
        config = {}
        logger.info("")
        logger.info("  [DRY RUN] Stages that would execute: %s", stages)
        logger.info("  No API calls, file writes, or DB inserts will occur.")
        orchestrate(stages, config, args.fail_fast, dry_run=True, logger=logger)
        return 0

    try:
        config = load_config(args.config)
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
        logger.critical("Config error — aborting: %s", e)
        return 2

    results = orchestrate(
        stages    = stages,
        config    = config,
        fail_fast = args.fail_fast,
        dry_run   = False,
        logger    = logger,
    )

    total_s = (datetime.now(tz=timezone.utc) - run_ts).total_seconds()
    print_summary(results, total_s, logger)

    statuses = {r.status for r in results}
    if "FAILED" in statuses and not any(r.status in ("SUCCESS", "PARTIAL") for r in results):
        return 2
    if statuses - {"SUCCESS", "NO_OP"}:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
