"""
extract_weather.py
==================
Stage 1 of the Weather Analytics ETL pipeline.

Responsibility: fetch live weather data from OpenWeatherMap and write a
timestamped raw JSON file to data/raw/.

Designed to be called by master_pipeline.py → run_extract(), but also
runnable standalone for debugging (python extract_weather.py).

Public API (consumed by master_pipeline.py)
-------------------------------------------
  build_session(retries, backoff_factor, timeout) → requests.Session
  fetch_weather(city, api_key, session, logger, units) → (data | None, err | None)
  save_output(output_data, output_dir, timestamp, logger) → str
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── Shared definitions ────────────────────────────────────────────────────────
from pipeline_config import (
    DEFAULTS,
    OPENWEATHER_ERROR_MESSAGES,
    get_logger,
    load_config,
)


# ─────────────────────────────────────────────────────────────────────────────
# HTTP Session
# ─────────────────────────────────────────────────────────────────────────────

def build_session(
    retries:        int   = DEFAULTS.http_retries,
    backoff_factor: float = DEFAULTS.http_backoff_factor,
    timeout:        int   = DEFAULTS.http_timeout_seconds,
) -> requests.Session:
    """
    Create a requests.Session with automatic retries and connection pooling.

    Retries on: 429, 500, 502, 503, 504.
    Does NOT retry on 401/404 (client errors that won't resolve on retry).
    """
    session = requests.Session()

    retry_strategy = Retry(
        total          = retries,
        backoff_factor = backoff_factor,
        status_forcelist = [429, 500, 502, 503, 504],
        allowed_methods  = ["GET"],
        raise_on_status  = False,
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://",  adapter)

    # Bake the timeout into every request so callers never have to pass it
    session.request = lambda method, url, **kwargs: requests.Session.request(
        session, method, url, timeout=kwargs.pop("timeout", timeout), **kwargs
    )

    return session


# ─────────────────────────────────────────────────────────────────────────────
# Weather Fetcher
# ─────────────────────────────────────────────────────────────────────────────

def fetch_weather(
    city:    str,
    api_key: str,
    session: requests.Session,
    logger,                      # logging.Logger — typed loosely to avoid circular
    units:   str = DEFAULTS.units,
) -> tuple[dict | None, dict | None]:
    """
    Fetch current weather for a single city.

    Returns:
        (weather_data, None)   on success
        (None, error_record)   on failure

    Never raises — all exceptions are caught and returned as error records
    so the pipeline can continue with remaining cities.
    """
    params = {"q": city, "appid": api_key, "units": units}

    try:
        response = session.get(DEFAULTS.openweather_base_url, params=params)

    except requests.exceptions.ConnectionError as e:
        logger.error("[ERROR] %s — Connection error: %s", city, e)
        return None, {"city": city, "error_type": "ConnectionError", "error": str(e)}

    except requests.exceptions.Timeout:
        logger.error("[ERROR] %s — Request timed out.", city)
        return None, {"city": city, "error_type": "Timeout", "error": "Request timed out"}

    except requests.exceptions.RequestException as e:
        logger.error("[ERROR] %s — Unexpected request error: %s", city, e)
        return None, {"city": city, "error_type": "RequestException", "error": str(e)}

    if response.status_code == 200:
        try:
            weather_data = response.json()
        except ValueError:
            logger.error("[ERROR] %s — 200 OK but body is not valid JSON.", city)
            return None, {
                "city": city,
                "error_type": "InvalidJSON",
                "error": "200 OK but response body is not JSON",
            }
        logger.info("[SUCCESS] %s", city)
        return weather_data, None

    # Non-200 response
    friendly_msg = OPENWEATHER_ERROR_MESSAGES.get(
        response.status_code,
        f"HTTP {response.status_code}",
    )
    logger.warning(
        "[FAILED] %s — %s (status=%d, body=%s)",
        city, friendly_msg, response.status_code, response.text[:200],
    )
    return None, {
        "city":          city,
        "error_type":    "HTTPError",
        "status_code":   response.status_code,
        "reason":        friendly_msg,
        "response_body": response.text[:500],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Output Writer
# ─────────────────────────────────────────────────────────────────────────────

def save_output(
    output_data: dict,
    output_dir:  str,
    timestamp:   str,
    logger,
) -> str:
    """
    Persist the ingestion result to a timestamped JSON file.

    Returns:
        Absolute path of the written file as a string.

    Raises:
        OSError — if the directory cannot be created or the file cannot be written.
    """
    out_dir = Path(output_dir)

    try:
        out_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise OSError(f"Cannot create output directory '{out_dir}': {e}") from e

    output_file = out_dir / f"weather_{timestamp}.json"

    try:
        with output_file.open("w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=4, ensure_ascii=False)
    except OSError as e:
        raise OSError(f"Failed to write output file '{output_file}': {e}") from e

    logger.debug("Raw output written to %s", output_file)
    return str(output_file)


# ─────────────────────────────────────────────────────────────────────────────
# Standalone entry point
# Only used when running:  python extract_weather.py
# master_pipeline.py calls the functions above directly via run_extract().
# ─────────────────────────────────────────────────────────────────────────────

def _standalone_main() -> int:
    """
    Run the extract stage in isolation.

    Exit codes:
        0 — all cities succeeded
        1 — partial failure (some cities failed)
        2 — fatal error (config/IO issues, nothing written)
    """
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger    = get_logger(
        name     = "weather_extract",
        log_file = f"weather_extract_{datetime.now().strftime('%Y%m%d')}.log",
    )

    logger.info("===== Extract Stage (standalone) | %s =====", timestamp)

    try:
        config = load_config()
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
        logger.critical("Config error — aborting: %s", e)
        return 2

    api_key    = config["api_key"].strip()
    cities     = [c.strip() for c in config["cities"]]
    units      = config.get("units",                  DEFAULTS.units)
    output_dir = config.get("output_dir",             DEFAULTS.raw_folder)
    delay      = float(config.get("request_delay_seconds", DEFAULTS.request_delay_seconds))

    session = build_session(
        retries        = config.get("http_retries",        DEFAULTS.http_retries),
        backoff_factor = config.get("http_backoff_factor", DEFAULTS.http_backoff_factor),
        timeout        = config.get("http_timeout_seconds", DEFAULTS.http_timeout_seconds),
    )

    successful_cities: list[dict] = []
    failed_cities:     list[dict] = []

    for city in cities:
        weather_data, error_record = fetch_weather(city, api_key, session, logger, units)
        if weather_data is not None:
            successful_cities.append(weather_data)
        else:
            failed_cities.append(error_record)
        if delay > 0:
            time.sleep(delay)

    if not successful_cities:
        logger.critical("All city requests failed — no data written.")
        return 2

    output_data = {
        "ingestion_timestamp": timestamp,
        "total_requested":     len(cities),
        "successful_records":  len(successful_cities),
        "failed_records":      len(failed_cities),
        "units":               units,
        "weather_data":        successful_cities,
        "failed_cities":       failed_cities,
    }

    try:
        output_file = save_output(output_data, output_dir, timestamp, logger)
    except OSError as e:
        logger.critical("Could not save output — aborting: %s", e)
        return 2

    logger.info("Success: %d/%d | Output: %s", len(successful_cities), len(cities), output_file)
    if failed_cities:
        logger.warning("Failed: %s", [fc.get("city") for fc in failed_cities])

    return 0 if not failed_cities else 1


if __name__ == "__main__":
    sys.exit(_standalone_main())
