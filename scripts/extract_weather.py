"""
extract_weather.py
==================
Stage 1 of the Weather Analytics ETL pipeline.

Responsibility: fetch live weather data from Open-Meteo (no API key needed)
and write a timestamped raw JSON file to data/raw/.

Bug fix (cities resolving outside India)
-----------------------------------------
Open-Meteo's free geocoding endpoint does a GLOBAL, relevance-ranked fuzzy
search. For a tehsil name that happens to coincide with a more "important"
place elsewhere (by population), `count=1` can silently return the foreign
match instead of the Maharashtra one — even though the spelling is identical.

The fix has two layers:
  1. PRIMARY — for the real city list, skip geocoding entirely. Open-Meteo's
     forecast endpoint just needs lat/lon, and maharashtra_tehsils_final.csv
     already carries pre-resolved, validated coordinates for every tehsil
     (see its 'match_type' column). load_city_list() reads those directly,
     so there is no name-search step — and therefore nothing that can match
     the wrong country — for any of the 224 production cities.
  2. SAFETY NET — the legacy name-based geocode_city() path (kept only for
     ad-hoc/test entries in config['cities'], e.g. an intentionally-invalid
     city used to exercise error handling) now passes Open-Meteo's
     documented `countryCode` filter, scoped to DEFAULTS.geocode_country_code
     ("IN" by default), so even that path can no longer drift to another country.

Designed to be called by master_pipeline.py → run_extract(), but also
runnable standalone for debugging (python extract_weather.py).

Public API (consumed by master_pipeline.py)
-------------------------------------------
  build_session(retries, backoff_factor, timeout)                    → requests.Session
  load_geocode_cache(cache_file)                                      → dict
  save_geocode_cache(cache, cache_file)                               → None
  load_city_list(csv_path, logger)                                    → list[dict]
  geocode_city(city, session, logger, cache, country_code)            → (location | None, err | None)
  fetch_weather(city, session, logger, geocode_cache, units, ...)     → (data | None, err | None)
  fetch_weather_for_target(target, session, logger, units)            → (data | None, err | None)
  save_output(output_data, output_dir, timestamp, logger)             → str
"""

from __future__ import annotations

import csv
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
# City list  — tehsil name + ALREADY-RESOLVED lat/lon/district/division/region
# This is the primary source of cities. No geocoding happens for any row
# loaded from here, which is what eliminates the wrong-country bug.
# ─────────────────────────────────────────────────────────────────────────────

CITY_LIST_REQUIRED_COLUMNS = {"tehsil", "country", "lat", "lon"}
CITY_LIST_OPTIONAL_COLUMNS = ("district", "division", "region")


def load_city_list(csv_path: str, logger) -> list[dict]:
    """
    Load tehsils/cities with pre-resolved lat/lon from a CSV such as
    maharashtra_tehsils_final.csv. No geocoding is performed — lat/lon
    (and district/division/region, when present) are taken as-is from
    the file.

    Required columns: tehsil, country, lat, lon.
    Optional columns carried through if present: district, division, region.

    Rows missing a required field, or with a non-numeric lat/lon, are
    skipped (and counted) rather than raising — one bad row in a 224-row
    file shouldn't abort the whole run.

    Raises:
        FileNotFoundError — csv_path doesn't exist
        ValueError         — required columns missing from the header
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"City list file not found: {path.resolve()}")

    targets: list[dict] = []
    skipped = 0

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        header = set(reader.fieldnames or [])
        missing = CITY_LIST_REQUIRED_COLUMNS - header
        if missing:
            raise ValueError(f"City list '{csv_path}' is missing required columns: {missing}")

        for row in reader:
            tehsil   = (row.get("tehsil")  or "").strip()
            country  = (row.get("country") or "").strip()
            lat_raw  = (row.get("lat")     or "").strip()
            lon_raw  = (row.get("lon")     or "").strip()

            if not tehsil or not country or not lat_raw or not lon_raw:
                skipped += 1
                continue

            try:
                latitude  = float(lat_raw)
                longitude = float(lon_raw)
            except ValueError:
                skipped += 1
                continue

            target = {
                "city":      tehsil,
                "country":   country,
                "latitude":  latitude,
                "longitude": longitude,
            }
            for col in CITY_LIST_OPTIONAL_COLUMNS:
                val = (row.get(col) or "").strip()
                target[col] = val or None

            targets.append(target)

    if skipped:
        logger.warning("City list: skipped %d row(s) missing required fields in '%s'.", skipped, csv_path)
    logger.info("City list: loaded %d cities from %s", len(targets), csv_path)
    return targets


# ─────────────────────────────────────────────────────────────────────────────
# Geocode cache  — city name → {latitude, longitude, country}
# Only used by the legacy name-based path (geocode_city/fetch_weather), kept
# for ad-hoc/test entries in config['cities']. Cities loaded via
# load_city_list() never touch this — they already have coordinates.
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
# Geocoding  — city name → lat/lon  (legacy / ad-hoc path only)
# ─────────────────────────────────────────────────────────────────────────────

def geocode_city(
    city:         str,
    session:      requests.Session,
    logger,
    cache:        dict,
    country_code: str = DEFAULTS.geocode_country_code,
) -> tuple[dict | None, dict | None]:
    """
    Resolve a city name to {latitude, longitude, country} via Open-Meteo's
    free geocoding API, scoped to `country_code` (Open-Meteo's documented
    `countryCode` filter — an ISO-3166-1 alpha2 code, e.g. "IN") so a name
    collision with a place in another country can't be returned. Pass
    country_code="" to disable the filter.

    Cached on `cache` (mutated in place), keyed by city+country_code, so the
    same lookup is only ever made once across runs.

    Returns:
        (location_dict, None)  on success
        (None, error_record)   on failure (never raises)
    """
    cache_key = f"{city.strip().lower()}|{country_code or 'any'}"
    if cache_key in cache:
        return cache[cache_key], None

    params = {"name": city, "count": 1, "language": "en", "format": "json"}
    if country_code:
        params["countryCode"] = country_code

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
        logger.warning("[GEOCODE FAILED] %s — no match (country_code=%s).", city, country_code or "any")
        return None, {
            "city": city, "error_type": "CityNotFound",
            "error": f"Open-Meteo geocoder found no match for '{city}' (country_code={country_code or 'any'}).",
        }

    top = results[0]
    location = {
        "latitude":  top.get("latitude"),
        "longitude": top.get("longitude"),
        "country":   top.get("country_code") or top.get("country"),
    }

    cache[cache_key] = location
    logger.info(
        "[GEOCODE OK] %s -> (%.4f, %.4f) [%s]",
        city, location["latitude"], location["longitude"], location["country"],
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


def _request_forecast(
    latitude:  float,
    longitude: float,
    session:   requests.Session,
    logger,
    label:     str,
) -> tuple[dict | None, dict | None]:
    """
    Shared HTTP call to Open-Meteo's forecast endpoint for one lat/lon.
    Used by both fetch_weather() (geocoded path) and fetch_weather_for_target()
    (CSV path with pre-resolved coordinates). `label` is just for logging/error
    messages — it's whatever the caller calls this location (city or tehsil name).

    Returns:
        (payload_dict, None)   on success — payload has 'current'/'hourly'/'daily'
        (None, error_record)   on failure (never raises)
    """
    params = {
        "latitude":           latitude,
        "longitude":          longitude,
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
        logger.error("[ERROR] %s — Connection error: %s", label, e)
        return None, {"city": label, "error_type": "ConnectionError", "error": str(e)}

    except requests.exceptions.Timeout:
        logger.error("[ERROR] %s — Request timed out.", label)
        return None, {"city": label, "error_type": "Timeout", "error": "Request timed out"}

    except requests.exceptions.RequestException as e:
        logger.error("[ERROR] %s — Unexpected request error: %s", label, e)
        return None, {"city": label, "error_type": "RequestException", "error": str(e)}

    if response.status_code != 200:
        friendly_msg = OPEN_METEO_ERROR_MESSAGES.get(
            response.status_code, f"HTTP {response.status_code}",
        )
        logger.warning(
            "[FAILED] %s — %s (status=%d, body=%s)",
            label, friendly_msg, response.status_code, response.text[:200],
        )
        return None, {
            "city":          label,
            "error_type":    "HTTPError",
            "status_code":   response.status_code,
            "reason":        friendly_msg,
            "response_body": response.text[:500],
        }

    try:
        payload = response.json()
    except ValueError:
        logger.error("[ERROR] %s — 200 OK but body is not valid JSON.", label)
        return None, {
            "city": label, "error_type": "InvalidJSON",
            "error": "200 OK but response body is not JSON",
        }

    if not payload.get("current"):
        logger.warning("[FAILED] %s — empty 'current' payload.", label)
        return None, {
            "city": label, "error_type": "EmptyPayload",
            "error": "Open-Meteo returned no 'current' weather block.",
        }

    return payload, None


def fetch_weather(
    city:           str,
    session:        requests.Session,
    logger,                      # logging.Logger — typed loosely to avoid circular
    geocode_cache:  dict,
    units:          str = DEFAULTS.units,                       # kept for interface
                                                                  # compatibility; see note.
    country_code:   str = DEFAULTS.geocode_country_code,
) -> tuple[dict | None, dict | None]:
    """
    Fetch current weather for a single city BY NAME (geocode + forecast).

    Intended for ad-hoc / test entries in config['cities'] only — the
    production city list should go through fetch_weather_for_target()
    instead, which has no geocoding step at all.

    Note on `units`: every downstream column is explicitly named for its unit
    (temp_celsius, wind_speed_mps, ...), so this always requests metric units
    from Open-Meteo regardless of the `units` value, to keep those column
    names accurate. The parameter is kept only so callers don't need to change.

    Returns:
        (weather_record, None)   on success
        (None, error_record)     on failure
    """
    location, geo_err = geocode_city(city, session, logger, geocode_cache, country_code)
    if location is None:
        return None, geo_err

    payload, err = _request_forecast(location["latitude"], location["longitude"], session, logger, city)
    if payload is None:
        return None, err

    weather_data = {
        "city":      city,
        "country":   location.get("country"),
        "latitude":  location.get("latitude"),
        "longitude": location.get("longitude"),
        "district":  None,
        "division":  None,
        "region":    None,
        "current":   payload.get("current", {}),
        "hourly":    payload.get("hourly", {}),
        "daily":     payload.get("daily", {}),
    }
    logger.info("[SUCCESS] %s", city)
    return weather_data, None


def fetch_weather_for_target(
    target:  dict,
    session: requests.Session,
    logger,
    units:   str = DEFAULTS.units,   # kept for interface compatibility; see fetch_weather().
) -> tuple[dict | None, dict | None]:
    """
    Fetch current weather for one row from load_city_list() — lat/lon are
    already known, so this makes exactly ONE HTTP call (no geocoding, and
    therefore no possibility of resolving to the wrong country).

    `target` is one dict as produced by load_city_list():
        {city, country, latitude, longitude, district, division, region}

    Returns:
        (weather_record, None)   on success
        (None, error_record)     on failure
    """
    label = target["city"]
    payload, err = _request_forecast(target["latitude"], target["longitude"], session, logger, label)
    if payload is None:
        return None, err

    weather_data = {
        "city":      target["city"],
        "country":   target.get("country"),
        "latitude":  target["latitude"],
        "longitude": target["longitude"],
        "district":  target.get("district"),
        "division":  target.get("division"),
        "region":    target.get("region"),
        "current":   payload.get("current", {}),
        "hourly":    payload.get("hourly", {}),
        "daily":     payload.get("daily", {}),
    }
    logger.info("[SUCCESS] %s", label)
    return weather_data, None


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

    Pulls cities from up to two sources, both optional but at least one
    required (load_config() enforces this):
      - config['city_list_file']  → load_city_list()        → fetch_weather_for_target()
      - config['cities']          → geocode_city() + forecast → fetch_weather()

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

    city_list_file     = config.get("city_list_file",         DEFAULTS.city_list_file)
    adhoc_cities        = [c.strip() for c in config.get("cities", [])]
    units              = config.get("units",                  DEFAULTS.units)
    output_dir         = config.get("output_dir",             DEFAULTS.raw_folder)
    delay              = float(config.get("request_delay_seconds", DEFAULTS.request_delay_seconds))
    geocode_cache_file = config.get("geocode_cache_file",     DEFAULTS.geocode_cache_file)
    country_code       = config.get("geocode_country_code",   DEFAULTS.geocode_country_code)

    targets: list[dict] = []
    if city_list_file:
        try:
            targets = load_city_list(city_list_file, logger)
        except (FileNotFoundError, ValueError) as e:
            logger.critical("City list error — aborting: %s", e)
            return 2

    if not targets and not adhoc_cities:
        logger.critical("No cities configured — set 'city_list_file' and/or 'cities' in config.json.")
        return 2

    session       = build_session(
        retries        = config.get("http_retries",        DEFAULTS.http_retries),
        backoff_factor = config.get("http_backoff_factor", DEFAULTS.http_backoff_factor),
        timeout        = config.get("http_timeout_seconds", DEFAULTS.http_timeout_seconds),
    )
    geocode_cache = load_geocode_cache(geocode_cache_file) if adhoc_cities else {}

    successful_cities: list[dict] = []
    failed_cities:     list[dict] = []

    # Primary source: pre-resolved coordinates, no geocoding.
    for target in targets:
        weather_data, error_record = fetch_weather_for_target(target, session, logger, units)
        if weather_data is not None:
            successful_cities.append(weather_data)
        else:
            failed_cities.append(error_record)
        if delay > 0:
            time.sleep(delay)

    # Secondary source: ad-hoc / test names, geocoded (country-scoped).
    for city in adhoc_cities:
        weather_data, error_record = fetch_weather(city, session, logger, geocode_cache, units, country_code)
        if weather_data is not None:
            successful_cities.append(weather_data)
        else:
            failed_cities.append(error_record)
        if delay > 0:
            time.sleep(delay)

    if adhoc_cities:
        try:
            save_geocode_cache(geocode_cache, geocode_cache_file)
        except OSError as e:
            logger.warning("Could not persist geocode cache: %s", e)

    if not successful_cities:
        logger.critical("All city requests failed — no data written.")
        return 2

    total_requested = len(targets) + len(adhoc_cities)
    output_data = {
        "ingestion_timestamp": timestamp,
        "total_requested":     total_requested,
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

    logger.info("Success: %d/%d | Output: %s", len(successful_cities), total_requested, output_file)
    if failed_cities:
        logger.warning("Failed: %s", [fc.get("city") for fc in failed_cities])

    return 0 if not failed_cities else 1


if __name__ == "__main__":
    sys.exit(_standalone_main())