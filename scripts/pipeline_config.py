"""
pipeline_config.py
==================
Single source of truth for the Weather Analytics ETL pipeline.

Every other module (extract_weather, transform, load_weather, master_pipeline)
imports from here instead of duplicating definitions. Nothing in this file
has module-level side-effects: no logging.basicConfig(), no global logger,
no file I/O. All setup is done inside functions so imports are always safe.

Public surface
--------------
  StageResult          — typed outcome dataclass passed between stages
  load_config()        — validated JSON config loader
  get_logger()         — named-logger factory (call once per module)
  STAGE_ORDER          — canonical ["extract", "transform", "load"]
  Defaults             — dataclass of all path/timing defaults
"""

from __future__ import annotations

import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

STAGE_ORDER: list[str] = ["extract", "transform", "load"]

REQUIRED_CONFIG_KEYS: frozenset[str] = frozenset({"api_key", "cities"})


# ─────────────────────────────────────────────────────────────────────────────
# Defaults  — all path/timing knobs in one place.
#             Each stage reads from config first, falls back to these.
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Defaults:
    # Paths
    config_file:      str = "config/config.json"
    log_dir:          str = "logs"
    raw_folder:       str = "data/raw"
    processed_folder: str = "data/processed"
    metadata_file:    str = "data/processed_metadata/processed_files.json"
    database_file:    str = "database/weather.db"
    sql_file:         str = "sql/create_tables.sql"
    load_metadata_file: str = "data/load_metadata/loaded_files.json"

    # Extract
    units:                str   = "metric"
    request_delay_seconds: float = 0.2
    http_retries:         int   = 3
    http_backoff_factor:  float = 0.5
    http_timeout_seconds: int   = 10

    # OpenWeather
    openweather_base_url: str = "https://api.openweathermap.org/data/2.5/weather"


DEFAULTS = Defaults()


# ─────────────────────────────────────────────────────────────────────────────
# StageResult  — typed outcome object for each pipeline stage
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class StageResult:
    """
    Captures outcome, timing, and file handoff for one pipeline stage.

    files_out is passed to the NEXT stage as its files_in, enabling
    explicit in-memory handoff instead of folder re-scanning.
    """

    stage:           str
    status:          str                       # SUCCESS | PARTIAL | FAILED | NO_OP
    started_at:      datetime
    finished_at:     Optional[datetime] = None
    files_in:        list[str]          = field(default_factory=list)
    files_out:       list[str]          = field(default_factory=list)
    rows_processed:  int                = 0
    error:           Optional[str]      = None

    @property
    def duration_s(self) -> float:
        if self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return 0.0

    def complete(self, status: str, **kwargs) -> "StageResult":
        """Finalise the result in one line. Returns self for chaining."""
        self.finished_at = datetime.now(tz=timezone.utc)
        self.status = status
        for k, v in kwargs.items():
            setattr(self, k, v)
        return self


# ─────────────────────────────────────────────────────────────────────────────
# Config loader
# ─────────────────────────────────────────────────────────────────────────────

def load_config(config_path: str = DEFAULTS.config_file) -> dict:
    """
    Load and validate the project config.json.

    Raises:
        FileNotFoundError   — file is missing
        json.JSONDecodeError — file is not valid JSON
        ValueError           — required keys absent or values invalid
    """
    path = Path(config_path)

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path.resolve()}")

    with path.open("r", encoding="utf-8") as f:
        try:
            config = json.load(f)
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                f"Invalid JSON in config '{config_path}': {e.msg}", e.doc, e.pos
            ) from e

    missing = REQUIRED_CONFIG_KEYS - config.keys()
    if missing:
        raise ValueError(f"Config is missing required keys: {missing}")

    if not isinstance(config["api_key"], str) or not config["api_key"].strip():
        raise ValueError("Config 'api_key' must be a non-empty string.")

    if not isinstance(config["cities"], list) or not config["cities"]:
        raise ValueError("Config 'cities' must be a non-empty list.")

    bad_cities = [c for c in config["cities"] if not isinstance(c, str) or not c.strip()]
    if bad_cities:
        raise ValueError(f"Config 'cities' contains invalid entries: {bad_cities}")

    return config


# ─────────────────────────────────────────────────────────────────────────────
# Logger factory  — NO module-level side effects.
#                   Call get_logger() inside functions, never at import time.
# ─────────────────────────────────────────────────────────────────────────────

def get_logger(
    name:      str,
    log_file:  str,
    log_dir:   str = DEFAULTS.log_dir,
) -> logging.Logger:
    """
    Return a named logger that writes DEBUG+ to `log_file` and INFO+ to stdout.

    Safe to call multiple times — guards against double-adding handlers.
    Does NOT touch the root logger or call logging.basicConfig().

    Args:
        name:     logger name (e.g. "weather_extract", "weather_load")
        log_file: filename inside log_dir (e.g. "extract.log")
        log_dir:  directory for log files (created if absent)
    """
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)

    if logger.handlers:          # already configured — return as-is
        return logger

    logger.setLevel(logging.DEBUG)
    logger.propagate = False     # don't bubble up to root logger

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(Path(log_dir) / log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


# ─────────────────────────────────────────────────────────────────────────────
# OpenWeather error map  — shared by extract_weather and (if needed) tests
# ─────────────────────────────────────────────────────────────────────────────

OPENWEATHER_ERROR_MESSAGES: dict[int, str] = {
    401: "Invalid or missing API key.",
    404: "City not found.",
    429: "API rate limit exceeded.",
    500: "OpenWeatherMap server error.",
}
