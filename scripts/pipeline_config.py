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

REQUIRED_CONFIG_KEYS: frozenset[str] = frozenset({"cities"})


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
    geocode_cache_file: str = "data/geocode_cache.json"

    # Extract
    units:                str   = "metric"   # informational only — see extract_weather.py
    request_delay_seconds: float = 0.2
    http_retries:         int   = 3
    http_backoff_factor:  float = 0.5
    http_timeout_seconds: int   = 10

    # Open-Meteo (no API key required)
    geocoding_base_url: str = "https://geocoding-api.open-meteo.com/v1/search"
    forecast_base_url:  str = "https://api.open-meteo.com/v1/forecast"


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
# Open-Meteo error map  — shared by extract_weather and (if needed) tests
# ─────────────────────────────────────────────────────────────────────────────

OPEN_METEO_ERROR_MESSAGES: dict[int, str] = {
    400: "Invalid request parameters (bad coordinates or variable name).",
    404: "Endpoint not found.",
    429: "Open-Meteo rate limit exceeded.",
    500: "Open-Meteo server error.",
    502: "Open-Meteo gateway error.",
    503: "Open-Meteo service temporarily unavailable.",
}


# ─────────────────────────────────────────────────────────────────────────────
# WMO weather_code → OWM-style (main, description, id)
# ─────────────────────────────────────────────────────────────────────────────
# Open-Meteo reports the WMO 4677 weather code, not OpenWeatherMap's own
# condition IDs. This table maps WMO codes onto OWM-style categories/ids so
# downstream columns (weather_main / weather_desc / weather_id) and the
# WEATHER_SEVERITY_MAP in transform.py keep working unchanged. Ported from
# maharashtra_weather_pipeline.ipynb — these are close analogues, not
# official OWM ids.
# ─────────────────────────────────────────────────────────────────────────────

WMO_CODE_MAP: dict[int, tuple[str, str, int]] = {
    0:  ("Clear",        "clear sky",                     800),
    1:  ("Clear",        "mainly clear",                  801),
    2:  ("Clouds",       "partly cloudy",                 802),
    3:  ("Clouds",       "overcast",                       804),
    45: ("Fog",          "fog",                            741),
    48: ("Fog",          "depositing rime fog",            741),
    51: ("Drizzle",      "light drizzle",                  300),
    53: ("Drizzle",      "moderate drizzle",               301),
    55: ("Drizzle",      "dense drizzle",                  302),
    56: ("Drizzle",      "light freezing drizzle",         311),
    57: ("Drizzle",      "dense freezing drizzle",         312),
    61: ("Rain",         "slight rain",                    500),
    63: ("Rain",         "moderate rain",                  501),
    65: ("Rain",         "heavy rain",                     502),
    66: ("Rain",         "light freezing rain",            511),
    67: ("Rain",         "heavy freezing rain",            511),
    71: ("Snow",         "slight snow fall",                600),
    73: ("Snow",         "moderate snow fall",              601),
    75: ("Snow",         "heavy snow fall",                 602),
    77: ("Snow",         "snow grains",                     612),
    80: ("Rain",         "slight rain showers",             520),
    81: ("Rain",         "moderate rain showers",           521),
    82: ("Rain",         "violent rain showers",            522),
    85: ("Snow",         "slight snow showers",             620),
    86: ("Snow",         "heavy snow showers",              622),
    95: ("Thunderstorm", "thunderstorm",                    211),
    96: ("Thunderstorm", "thunderstorm with slight hail",   230),
    99: ("Thunderstorm", "thunderstorm with heavy hail",    232),
}


def map_weather_code(code) -> tuple[str, str, int]:
    """
    Translate a WMO weather_code into (weather_main, weather_desc, weather_id).

    Returns ("Unknown", "unknown", 900) for None/unrecognised codes, mirroring
    the fallback used in the historical-backfill notebook.
    """
    if code is None:
        return ("Unknown", "unknown", 900)
    try:
        return WMO_CODE_MAP.get(int(code), ("Unknown", f"unmapped code {code}", 900))
    except (TypeError, ValueError):
        return ("Unknown", "unknown", 900)