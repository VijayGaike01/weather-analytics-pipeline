"""
extract_weather.py
==================
Stage 1 of the Weather Analytics ETL pipeline.

Responsibility: fetch live weather data from Open-Meteo (no API key needed)
and write a timestamped raw JSON file to data/raw/.

Migration note (OpenWeatherMap → Open-Meteo)
---------------------------------------------
Open-Meteo's forecast endpoint takes latitude/longitude, not a city name, so
each city now goes through two calls:
  1. geocode_city()  — city name  → {latitude, longitude, country}
                        (resolved once, then cached on disk so re-runs don't
                        re-hit the geocoder for cities we already know)
  2. fetch_weather()  — latitude/longitude → current + daily weather

The per-city record written to the raw JSON keeps the SAME key names that
fetch_weather() always wrote (city/country/latitude/longitude/current/
hourly/daily), so transform.py's build_dataframe() doesn't need to change —
only flatten_weather() does. Column names below match the engineered
columns built in maharashtra_weather_pipeline.ipynb.

Designed to be called by master_pipeline.py → run_extract(), but also
runnable standalone for debugging (python extract_weather.py).

Public API (consumed by master_pipeline.py)
-------------------------------------------
  build_session(retries, backoff_factor, timeout)              → requests.Session
  load_geocode_cache(cache_file)                                → dict
  save_geocode_cache(cache, cache_file)                         → None
  geocode_city(city, session, logger, cache)                    → (location | None, err | None)
  fetch_weather(city, session, logger, geocode_cache, units)    → (data | None, err | None)
  save_output(output_data, output_dir, timestamp, logger)       → str
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
    OPEN_METEO_ERROR_MESSAGES,
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
    Does NOT retry on 400/404 (client errors that won't resolve on retry).
    Shared by both Open-Meteo endpoints (geocoding-api.* and api.*).
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
# Geocode cache  — city name → {latitude, longitude, country}
# Avoids re-hitting the geocoder every run and keeps a city's coordinates
# stable over time (important: the city's identity in the dataset shouldn't
# drift if Open-Meteo's fuzzy match ever returns a slightly different point).
# ─────────────────────────────────────────────────────────────────────────────

def load_geocode_cache(cache_file: str = DEFAULTS.geocode_cache_file) -> dict:
    """Return the on-disk geocode cache; {} if it doesn't exist yet."""
    path = Path(cache_file)
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def save_geocode_cache(cache: dict, cache_file: str = DEFAULTS.geocode_cache_file) -> None:
    """Persist the geocode cache. Failures are non-fatal (just logged by caller)."""
    path = Path(cache_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(cache, f, indent=4, ensure_ascii=False)


# ─────────────────────────────────────────────────────────────────────────────
# Geocoding  — city name → lat/lon
# ─────────────────────────────────────────────────────────────────────────────

def geocode_city(
    city:    str,
    session: requests.Session,
    logger,
    cache:   dict,
) -> tuple[dict | None, dict | None]:
    """
    Resolve a city name to {latitude, longitude, country} via Open-Meteo's
    free geocoding API. Cached on `cache` (mutated in place) so the same
    city is only ever looked up once across runs.

    Returns:
        (location_dict, None)  on success
        (None, error_record)   on failure (never raises)
    """
    cache_key = city.strip().lower()
    if cache_key in cache:
        return cache[cache_key], None

    params = {"name": city, "count": 1, "language": "en", "format": "json"}

    try:
        response = session.get(DEFAULTS.geocoding_base_url, params=params)

    except requests.exceptions.ConnectionError as e:
        logger.error("[GEOCODE ERROR] %s — Connection error: %s", city, e)
        return None, {"city": city, "error_type": "ConnectionError", "error": str(e)}

    except requests.exceptions.Timeout:
        logger.error("[GEOCODE ERROR] %s — Request timed out.", city)
        return None, {"city": city, "error_type": "Timeout", "error": "Request timed out"}

    except requests.exceptions.RequestException as e:
        logger.error("[GEOCODE ERROR] %s — Unexpected request error: %s", city, e)
        return None, {"city": city, "error_type": "RequestException", "error": str(e)}

    if response.status_code != 200:
        friendly_msg = OPEN_METEO_ERROR_MESSAGES.get(
            response.status_code, f"HTTP {response.status_code}",
        )
        logger.warning(
            "[GEOCODE FAILED] %s — %s (status=%d)",
            city, friendly_msg, response.status_code,
        )
        return None, {
            "city":          city,
            "error_type":    "HTTPError",
            "status_code":   response.status_code,
            "reason":        friendly_msg,
            "response_body": response.text[:500],
        }

    try:
        payload = response.json()
    except ValueError:
        logger.error("[GEOCODE ERROR] %s — 200 OK but body is not valid JSON.", city)
        return None, {
            "city": city, "error_type": "InvalidJSON",
            "error": "200 OK but response body is not JSON",
        }

    results = payload.get("results") or []
    if not results:
        logger.warning("[GEOCODE FAILED] %s — city not found.", city)
        return None, {
            "city": city, "error_type": "CityNotFound",
            "error": f"Open-Meteo geocoder found no match for '{city}'.",
        }

    top = results[0]
    location = {
        "latitude":  top.get("latitude"),
        "longitude": top.get("longitude"),
        "country":   top.get("country_code") or top.get("country"),
    }

    cache[cache_key] = location
    logger.info(
        "[GEOCODE OK] %s -> (%.4f, %.4f)", city, location["latitude"], location["longitude"],
    )
    return location, None


# ─────────────────────────────────────────────────────────────────────────────
# Weather Fetcher
# ─────────────────────────────────────────────────────────────────────────────

# Matches the historical-backfill notebook's HOURLY_VARS/DAILY_VARS, trimmed
# to what a single "right-now" snapshot needs. `visibility` is requested as
# hourly (forecast_days=1) because Open-Meteo's `current` block doesn't carry
# it — we pick out the entry matching `current.time` in flatten_weather().
CURRENT_VARS: str = (
    "temperature_2m,relative_humidity_2m,apparent_temperature,"
    "pressure_msl,surface_pressure,cloud_cover,"
    "wind_speed_10m,wind_direction_10m,weather_code,rain"
)
HOURLY_VARS:  str = "visibility"
DAILY_VARS:   str = "sunrise,sunset,temperature_2m_max,temperature_2m_min"


def fetch_weather(
    city:           str,
    session:        requests.Session,
    logger,                      # logging.Logger — typed loosely to avoid circular
    geocode_cache:  dict,
    units:          str = DEFAULTS.units,   # kept for interface compatibility;
                                             # see note below.
) -> tuple[dict | None, dict | None]:
    """
    Fetch current weather for a single city via Open-Meteo (geocode + forecast).

    Note on `units`: every downstream column is explicitly named for its unit
    (temp_celsius, wind_speed_mps, ...), so this always requests metric units
    from Open-Meteo regardless of the `units` value, to keep those column
    names accurate. The parameter is kept only so callers don't need to change.

    Returns:
        (weather_record, None)   on success
        (None, error_record)     on failure

    Never raises — all exceptions are caught and returned as error records
    so the pipeline can continue with remaining cities.
    """
    location, geo_err = geocode_city(city, session, logger, geocode_cache)
    if location is None:
        return None, geo_err

    params = {
        "latitude":           location["latitude"],
        "longitude":          location["longitude"],
        "current":            CURRENT_VARS,
        "hourly":             HOURLY_VARS,
        "daily":              DAILY_VARS,
        "forecast_days":      1,
        "timezone":           "UTC",
        "wind_speed_unit":    "ms",
        "temperature_unit":   "celsius",
        "precipitation_unit": "mm",
        "timeformat":         "iso8601",
    }

    try:
        response = session.get(DEFAULTS.forecast_base_url, params=params)

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
            payload = response.json()
        except ValueError:
            logger.error("[ERROR] %s — 200 OK but body is not valid JSON.", city)
            return None, {
                "city": city,
                "error_type": "InvalidJSON",
                "error": "200 OK but response body is not JSON",
            }

        if not payload.get("current"):
            logger.warning("[FAILED] %s — empty 'current' payload.", city)
            return None, {
                "city": city, "error_type": "EmptyPayload",
                "error": "Open-Meteo returned no 'current' weather block.",
            }

        # Normalised record — same shape every downstream stage expects.
        weather_data = {
            "city":      city,
            "country":   location.get("country"),
            "latitude":  location.get("latitude"),
            "longitude": location.get("longitude"),
            "current":   payload.get("current", {}),
            "hourly":    payload.get("hourly", {}),
            "daily":     payload.get("daily", {}),
        }

        logger.info("[SUCCESS] %s", city)
        return weather_data, None

    # Non-200 response
    friendly_msg = OPEN_METEO_ERROR_MESSAGES.get(
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

    cities             = [c.strip() for c in config["cities"]]
    units              = config.get("units",                  DEFAULTS.units)
    output_dir         = config.get("output_dir",             DEFAULTS.raw_folder)
    delay              = float(config.get("request_delay_seconds", DEFAULTS.request_delay_seconds))
    geocode_cache_file = config.get("geocode_cache_file",     DEFAULTS.geocode_cache_file)

    session       = build_session(
        retries        = config.get("http_retries",        DEFAULTS.http_retries),
        backoff_factor = config.get("http_backoff_factor", DEFAULTS.http_backoff_factor),
        timeout        = config.get("http_timeout_seconds", DEFAULTS.http_timeout_seconds),
    )
    geocode_cache = load_geocode_cache(geocode_cache_file)

    successful_cities: list[dict] = []
    failed_cities:     list[dict] = []

    for city in cities:
        weather_data, error_record = fetch_weather(city, session, logger, geocode_cache, units)
        if weather_data is not None:
            successful_cities.append(weather_data)
        else:
            failed_cities.append(error_record)
        if delay > 0:
            time.sleep(delay)

    try:
        save_geocode_cache(geocode_cache, geocode_cache_file)
    except OSError as e:
        logger.warning("Could not persist geocode cache: %s", e)

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