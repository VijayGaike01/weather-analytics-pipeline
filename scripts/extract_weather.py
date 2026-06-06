import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ─── Logging Setup ────────────────────────────────────────────────────────────

def setup_logging(log_dir: str = "logs") -> logging.Logger:
    """Configure structured logging to both file and stdout."""
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    log_file = Path(log_dir) / f"weather_ingestion_{datetime.now().strftime('%Y%m%d')}.log"

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger = logging.getLogger("weather_ingestion")
    logger.setLevel(logging.DEBUG)

    # File handler — full debug output
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)

    # Console handler — info and above
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


# ─── Config Loader ────────────────────────────────────────────────────────────

REQUIRED_CONFIG_KEYS = {"api_key", "cities"}

def load_config(config_path: str = "config/config.json") -> dict[str, Any]:
    """
    Load and validate config.json.

    Raises:
        FileNotFoundError: if the config file is missing.
        json.JSONDecodeError: if the file is not valid JSON.
        ValueError: if required keys are absent or values are invalid.
    """
    path = Path(config_path)

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path.resolve()}")

    with path.open("r", encoding="utf-8") as f:
        try:
            config = json.load(f)
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                f"Invalid JSON in config file '{config_path}': {e.msg}",
                e.doc,
                e.pos,
            ) from e

    missing = REQUIRED_CONFIG_KEYS - config.keys()
    if missing:
        raise ValueError(f"Config is missing required keys: {missing}")

    if not isinstance(config["api_key"], str) or not config["api_key"].strip():
        raise ValueError("Config 'api_key' must be a non-empty string.")

    if not isinstance(config["cities"], list) or not config["cities"]:
        raise ValueError("Config 'cities' must be a non-empty list.")

    invalid_cities = [c for c in config["cities"] if not isinstance(c, str) or not c.strip()]
    if invalid_cities:
        raise ValueError(f"Config 'cities' contains invalid entries: {invalid_cities}")

    return config


# ─── HTTP Session ─────────────────────────────────────────────────────────────

def build_session(
    retries: int = 3,
    backoff_factor: float = 0.5,
    timeout: int = 10,
) -> requests.Session:
    """
    Create a requests Session with automatic retries and connection pooling.

    Retries on: 429, 500, 502, 503, 504.
    Does NOT retry on 401/404 (client errors that won't resolve on retry).
    """
    session = requests.Session()

    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.request = lambda method, url, **kwargs: requests.Session.request(
        session, method, url, timeout=kwargs.pop("timeout", timeout), **kwargs
    )

    return session


# ─── Weather Fetcher ──────────────────────────────────────────────────────────

OPENWEATHER_BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

# Human-readable explanations for common OpenWeather error codes
OPENWEATHER_ERROR_MESSAGES: dict[int, str] = {
    401: "Invalid or missing API key.",
    404: "City not found.",
    429: "API rate limit exceeded.",
    500: "OpenWeatherMap server error.",
}

def fetch_weather(
    city: str,
    api_key: str,
    session: requests.Session,
    logger: logging.Logger,
    units: str = "metric",
) -> tuple[dict | None, dict | None]:
    """
    Fetch weather for a single city.

    Returns:
        (weather_data, None) on success.
        (None, error_record) on failure.
    """
    url = OPENWEATHER_BASE_URL
    params = {"q": city, "appid": api_key, "units": units}

    try:
        response = session.get(url, params=params)
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
            logger.error("[ERROR] %s — Response was 200 but body is not valid JSON.", city)
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
        "city": city,
        "error_type": "HTTPError",
        "status_code": response.status_code,
        "reason": friendly_msg,
        "response_body": response.text[:500],
    }


# ─── Output Writer ────────────────────────────────────────────────────────────

def save_output(
    output_data: dict,
    output_dir: str,
    timestamp: str,
    logger: logging.Logger,
) -> str:
    """
    Persist the ingestion result to a timestamped JSON file.

    Returns the path of the written file.

    Raises:
        OSError: if the directory cannot be created or the file cannot be written.
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

    logger.debug("Output written to %s", output_file)
    return str(output_file)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    """
    Entry point. Returns an exit code:
        0 — all cities succeeded
        1 — partial failure (some cities failed)
        2 — fatal error (config/IO issues, no data written)
    """
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger = setup_logging()

    logger.info("========== Weather Ingestion Started | %s ==========", timestamp)

    # ── Load config ──────────────────────────────────────────────────────────
    try:
        config = load_config()
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
        logger.critical("Config error — aborting: %s", e)
        return 2

    api_key: str = config["api_key"].strip()
    cities: list[str] = [c.strip() for c in config["cities"]]
    units: str = config.get("units", "metric")
    output_dir: str = config.get("output_dir", "data/raw")
    request_delay: float = float(config.get("request_delay_seconds", 0.2))

    logger.info("Cities to fetch: %d | Units: %s", len(cities), units)

    # ── Build HTTP session ────────────────────────────────────────────────────
    session = build_session(
        retries=config.get("http_retries", 3),
        backoff_factor=config.get("http_backoff_factor", 0.5),
        timeout=config.get("http_timeout_seconds", 10),
    )

    # ── Fetch weather data ────────────────────────────────────────────────────
    successful_cities: list[dict] = []
    failed_cities: list[dict] = []

    for city in cities:
        weather_data, error_record = fetch_weather(city, api_key, session, logger, units)

        if weather_data is not None:
            successful_cities.append(weather_data)
        else:
            failed_cities.append(error_record)

        # Polite delay to avoid hammering the API
        if request_delay > 0:
            time.sleep(request_delay)

    # ── Build output ──────────────────────────────────────────────────────────
    output_data = {
        "ingestion_timestamp": timestamp,
        "total_requested": len(cities),
        "successful_records": len(successful_cities),
        "failed_records": len(failed_cities),
        "units": units,
        "weather_data": successful_cities,
        "failed_cities": failed_cities,
    }

    # ── Persist output ────────────────────────────────────────────────────────
    try:
        output_file = save_output(output_data, output_dir, timestamp, logger)
    except OSError as e:
        logger.critical("Could not save output — aborting: %s", e)
        return 2

    # ── Summary ───────────────────────────────────────────────────────────────
    logger.info("========== Ingestion Summary ==========")
    logger.info("Cities Requested : %d", len(cities))
    logger.info("Success          : %d", len(successful_cities))
    logger.info("Failed           : %d", len(failed_cities))
    logger.info("Output File      : %s", output_file)
    logger.info("=======================================")

    if failed_cities:
        logger.warning(
            "Failed cities: %s",
            ", ".join(fc.get("city", "unknown") for fc in failed_cities),
        )

    return 0 if not failed_cities else 1


if __name__ == "__main__":
    sys.exit(main())
