"""
transform.py
============
Stage 2 of the Weather Analytics ETL pipeline.

Responsibility: flatten raw JSON files produced by extract_weather.py into
clean, enriched CSVs and write them to data/processed/.

Designed to be called by master_pipeline.py → run_transform(), but also
runnable standalone for debugging (python transform.py).

Public API (consumed by master_pipeline.py)
-------------------------------------------
  load_metadata(metadata_file)                          → dict
  save_metadata(metadata_file, metadata)                → None
  mark_as_processed(metadata_file, filename)            → None
  get_unprocessed_files(raw_folder, metadata)           → list[str]
  load_raw_json(filepath)                               → dict
  flatten_weather(record)                               → dict
  build_dataframe(wrapper)                              → pd.DataFrame
  clean_dataframe(df)                                   → pd.DataFrame
  add_derived_columns(df)                               → pd.DataFrame
  add_business_columns(df)                              → pd.DataFrame
  save_processed(df, source_filename, output_folder)    → str
  print_quality_report(df, filename)                    → None
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# ── Shared definitions ────────────────────────────────────────────────────────
from pipeline_config import DEFAULTS, get_logger, load_config


# ─────────────────────────────────────────────────────────────────────────────
# NOTE: No module-level logging setup here.
#       get_logger() is called inside each function/standalone entry point
#       so importing this module never causes side effects.
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# Metadata Tracker  — tracks which raw files have already been transformed
# ─────────────────────────────────────────────────────────────────────────────

def load_metadata(metadata_file: str) -> dict:
    """Return the processed-files tracking dict; returns empty dict on first run."""
    if Path(metadata_file).exists():
        with open(metadata_file, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"processed_files": []}


def save_metadata(metadata_file: str, metadata: dict) -> None:
    """Persist updated metadata back to disk."""
    Path(metadata_file).parent.mkdir(parents=True, exist_ok=True)
    with open(metadata_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=4)


def mark_as_processed(metadata_file: str, filename: str) -> None:
    """Add a filename to the processed list and persist."""
    metadata = load_metadata(metadata_file)
    if filename not in metadata["processed_files"]:
        metadata["processed_files"].append(filename)
    save_metadata(metadata_file, metadata)


def get_unprocessed_files(raw_folder: str, metadata: dict) -> list[str]:
    """
    Return only raw JSON filenames not yet in the processed list.

    Run 1 → [A, B, C]   (all new)
    Run 2 → [D]          (A, B, C already tracked)
    Run 3 → []           (nothing new)
    """
    all_files    = sorted(f for f in os.listdir(raw_folder) if f.endswith(".json"))
    already_done = set(metadata["processed_files"])
    return [f for f in all_files if f not in already_done]


# ─────────────────────────────────────────────────────────────────────────────
# Load Raw JSON
# ─────────────────────────────────────────────────────────────────────────────

def load_raw_json(filepath: str) -> dict:
    """Read and return the JSON wrapper produced by extract_weather."""
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


# ─────────────────────────────────────────────────────────────────────────────
# Flatten / Build DataFrame
# ─────────────────────────────────────────────────────────────────────────────

def flatten_weather(record: dict) -> dict:
    """
    Flatten a single city dict from the 'weather_data' list.
    Temperatures are Celsius because the extractor uses units=metric.
    """
    return {
        # Identity
        "city":               record.get("name"),
        "country":            record.get("sys", {}).get("country"),
        "latitude":           record.get("coord", {}).get("lat"),
        "longitude":          record.get("coord", {}).get("lon"),

        # Time
        "timestamp_utc":      datetime.utcfromtimestamp(
                                  record.get("dt", 0)
                              ).strftime("%Y-%m-%d %H:%M:%S"),
        "sunrise_utc":        datetime.utcfromtimestamp(
                                  record.get("sys", {}).get("sunrise", 0)
                              ).strftime("%Y-%m-%d %H:%M:%S"),
        "sunset_utc":         datetime.utcfromtimestamp(
                                  record.get("sys", {}).get("sunset", 0)
                              ).strftime("%Y-%m-%d %H:%M:%S"),

        # Weather condition
        "weather_main":       record.get("weather", [{}])[0].get("main"),
        "weather_desc":       record.get("weather", [{}])[0].get("description"),
        "weather_id":         record.get("weather", [{}])[0].get("id"),

        # Temperature (already Celsius from API)
        "temp_celsius":       record.get("main", {}).get("temp"),
        "feels_like_celsius": record.get("main", {}).get("feels_like"),
        "temp_min_celsius":   record.get("main", {}).get("temp_min"),
        "temp_max_celsius":   record.get("main", {}).get("temp_max"),

        # Atmosphere
        "humidity_pct":       record.get("main", {}).get("humidity"),
        "pressure_hpa":       record.get("main", {}).get("pressure"),
        "visibility_m":       record.get("visibility"),

        # Wind
        "wind_speed_mps":     record.get("wind", {}).get("speed"),
        "wind_deg":           record.get("wind", {}).get("deg"),

        # Clouds & rain
        "cloud_pct":          record.get("clouds", {}).get("all"),
        "rain_1h_mm":         record.get("rain", {}).get("1h", 0.0),
    }


def build_dataframe(wrapper: dict) -> pd.DataFrame:
    """
    Unpack the 'weather_data' list and flatten each city record.

    Wrapper structure (from extract_weather):
        {
            "ingestion_timestamp": "...",
            "weather_data":  [ {city_1}, {city_2}, ... ],
            "failed_cities": [ ... ]
        }
    """
    city_records = wrapper.get("weather_data", [])
    if not city_records:
        return pd.DataFrame()

    rows = []
    for record in city_records:
        row = flatten_weather(record)
        row["ingestion_timestamp"] = wrapper.get("ingestion_timestamp")
        rows.append(row)

    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────────────────────
# Cleaning
# ─────────────────────────────────────────────────────────────────────────────

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicates, remove rows missing identity fields, fix dtypes."""
    initial = len(df)

    df = df.drop_duplicates()
    df = df.dropna(subset=["city", "timestamp_utc"])

    # Fields that are legitimately absent (no rain, unknown wind direction, etc.)
    for col in ["rain_1h_mm", "wind_deg", "visibility_m"]:
        df[col] = df[col].fillna(0)

    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"])
    df["humidity_pct"]  = df["humidity_pct"].astype(float)
    df["cloud_pct"]     = df["cloud_pct"].astype(float)

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Derived / Engineering Columns
# ─────────────────────────────────────────────────────────────────────────────

def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Unit conversions and time features for analytics."""

    df["temp_fahrenheit"] = ((df["temp_celsius"] * 9 / 5) + 32).round(2)
    df["wind_speed_kmh"]  = (df["wind_speed_mps"] * 3.6).round(2)

    # Heat index approximation
    df["heat_index"] = (
        df["temp_celsius"]
        - (0.55 - 0.0055 * df["humidity_pct"]) * (df["temp_celsius"] - 14.5)
    ).round(2)

    # Time parts (useful for dashboard slicing)
    df["date"]        = df["timestamp_utc"].dt.date
    df["hour"]        = df["timestamp_utc"].dt.hour
    df["day_of_week"] = df["timestamp_utc"].dt.day_name()

    # Daylight duration
    df["sunrise_utc"]  = pd.to_datetime(df["sunrise_utc"])
    df["sunset_utc"]   = pd.to_datetime(df["sunset_utc"])
    df["daylight_hrs"] = (
        (df["sunset_utc"] - df["sunrise_utc"]).dt.seconds / 3600
    ).round(2)

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Business / Analytics Columns
# ─────────────────────────────────────────────────────────────────────────────

# Maps every OpenWeather 'main' value to one of four severity buckets
WEATHER_SEVERITY_MAP: dict[str, str] = {
    "Clear":        "Clear",
    "Clouds":       "Cloudy",
    "Mist":         "Cloudy",
    "Smoke":        "Cloudy",
    "Haze":         "Cloudy",
    "Dust":         "Cloudy",
    "Fog":          "Cloudy",
    "Sand":         "Cloudy",
    "Ash":          "Cloudy",
    "Drizzle":      "Rainy",
    "Rain":         "Rainy",
    "Thunderstorm": "Storm",
    "Snow":         "Storm",
    "Squall":       "Storm",
    "Tornado":      "Storm",
}


def add_business_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Dashboard-friendly categorical columns.

    temp_category    : Cold / Moderate / Hot        (based on temp_celsius)
    humidity_category: Low / Medium / High           (based on humidity_pct)
    weather_severity : Clear / Cloudy / Rainy / Storm (based on weather_main)
    """
    df["temp_category"] = pd.cut(
        df["temp_celsius"],
        bins=[-float("inf"), 15, 30, float("inf")],
        labels=["Cold", "Moderate", "Hot"],
    ).astype(str)

    df["humidity_category"] = pd.cut(
        df["humidity_pct"],
        bins=[-float("inf"), 40, 70, float("inf")],
        labels=["Low", "Medium", "High"],
    ).astype(str)

    df["weather_severity"] = (
        df["weather_main"]
        .map(WEATHER_SEVERITY_MAP)
        .fillna("Clear")          # fallback for any unmapped value
    )

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Output
# ─────────────────────────────────────────────────────────────────────────────

def save_processed(df: pd.DataFrame, source_filename: str, output_folder: str) -> str:
    """
    Save the processed DataFrame as a CSV.
    Output name mirrors the source file for easy lineage tracking:

        IN  → data/raw/weather_20260807_103000.json
        OUT → data/processed/weather_20260807_103000.csv
    """
    os.makedirs(output_folder, exist_ok=True)
    base_name   = Path(source_filename).stem
    output_path = os.path.join(output_folder, f"{base_name}.csv")
    df.to_csv(output_path, index=False)
    return output_path


def print_quality_report(df: pd.DataFrame, filename: str) -> None:
    """Print a per-file data quality summary to stdout."""
    null_total = int(df.isnull().sum().sum())
    null_cols  = df.columns[df.isnull().any()].tolist()

    print(f"\n   Quality Report: {filename}")
    print(f"      Rows Processed  : {len(df)}")
    print(f"      Unique Cities   : {df['city'].nunique()}")
    print(f"      Null Values     : {null_total}")
    print(f"      Null Columns    : {null_cols if null_cols else 'None'}")
    print(f"      Temp Breakdown  : {df['temp_category'].value_counts().to_dict()}")
    print(f"      Severity Split  : {df['weather_severity'].value_counts().to_dict()}")


# ─────────────────────────────────────────────────────────────────────────────
# Standalone entry point
# Only used when running:  python transform.py
# master_pipeline.py calls the functions above directly via run_transform().
# ─────────────────────────────────────────────────────────────────────────────

def _standalone_main() -> int:
    logger = get_logger(
        name     = "weather_transform",
        log_file = f"weather_transform_{datetime.now().strftime('%Y%m%d')}.log",
    )

    logger.info("===== Transform Stage (standalone) =====")

    try:
        config = load_config()
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
        logger.critical("Config error — aborting: %s", e)
        return 2

    raw_folder       = config.get("raw_folder",       DEFAULTS.raw_folder)
    processed_folder = config.get("processed_folder", DEFAULTS.processed_folder)
    metadata_file    = config.get("metadata_file",    DEFAULTS.metadata_file)

    metadata  = load_metadata(metadata_file)
    new_files = get_unprocessed_files(raw_folder, metadata)

    logger.info(
        "Files — total: %d | already processed: %d | new: %d",
        len([f for f in os.listdir(raw_folder) if f.endswith(".json")]),
        len(metadata.get("processed_files", [])),
        len(new_files),
    )

    if not new_files:
        logger.info("No new files to process. Already up to date.")
        return 0

    total_rows = 0

    for filename in new_files:
        filepath = os.path.join(raw_folder, filename)
        logger.info("Processing: %s", filename)

        try:
            wrapper = load_raw_json(filepath)
            df      = build_dataframe(wrapper)

            if df.empty:
                logger.warning("No records in %s — skipping.", filename)
                continue

            df = clean_dataframe(df)
            df = add_derived_columns(df)
            df = add_business_columns(df)

            output_path = save_processed(df, filename, processed_folder)
            mark_as_processed(metadata_file, filename)

            total_rows += len(df)
            logger.info("Saved: %s (%d rows)", output_path, len(df))
            print_quality_report(df, filename)

        except Exception as e:
            logger.error("Failed to process %s: %s", filename, e)

    logger.info("Done. Total rows written: %d", total_rows)
    return 0


if __name__ == "__main__":
    sys.exit(_standalone_main())
