import json
import os
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path


# ── Logging Setup ──────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/transform.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


# ── Change 5: Config Loader ────────────────────────────────────────────────────
def load_config(config_path: str = "config/config.json") -> dict:
    """Load paths and settings from the shared project config."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path.resolve()}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


# ── Change 1: Metadata Tracker ─────────────────────────────────────────────────
def load_metadata(metadata_file: str) -> dict:
    """Load the list of already-processed raw files."""
    if Path(metadata_file).exists():
        with open(metadata_file, "r", encoding="utf-8") as f:
            return json.load(f)
    # First run — no metadata yet
    return {"processed_files": []}


def save_metadata(metadata_file: str, metadata: dict) -> None:
    """Persist updated metadata back to disk."""
    Path(metadata_file).parent.mkdir(parents=True, exist_ok=True)
    with open(metadata_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=4)


def mark_as_processed(metadata_file: str, filename: str) -> None:
    """Add a filename to the processed list."""
    metadata = load_metadata(metadata_file)
    if filename not in metadata["processed_files"]:
        metadata["processed_files"].append(filename)
    save_metadata(metadata_file, metadata)
    logger.debug(f"Marked as processed: {filename}")


def get_unprocessed_files(raw_folder: str, metadata: dict) -> list[str]:
    """
    Return only raw JSON files not yet in the processed list.

    Run 1 → [A, B, C]  (all new)
    Run 2 → [D]        (A, B, C already tracked)
    Run 3 → []         (nothing new)
    """
    all_files    = sorted(f for f in os.listdir(raw_folder) if f.endswith(".json"))
    already_done = set(metadata["processed_files"])
    new_files    = [f for f in all_files if f not in already_done]

    logger.info(
        "Files — total: %d | already processed: %d | new: %d",
        len(all_files), len(already_done), len(new_files),
    )
    return new_files


# ── Load Raw JSON ──────────────────────────────────────────────────────────────
def load_raw_json(filepath: str) -> dict:
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    logger.info(f"Loaded: {filepath}")
    return data


# ── Flatten ONE city record ────────────────────────────────────────────────────
def flatten_weather(record: dict) -> dict:
    """
    Flatten a single city dict from the 'weather_data' list.
    Temperatures are Celsius because the extractor uses units=metric.
    """
    return {
        # ── Identity ──────────────────────────────────────────
        "city":               record.get("name"),
        "country":            record.get("sys", {}).get("country"),
        "latitude":           record.get("coord", {}).get("lat"),
        "longitude":          record.get("coord", {}).get("lon"),

        # ── Time ──────────────────────────────────────────────
        "timestamp_utc":      datetime.utcfromtimestamp(
                                  record.get("dt", 0)
                              ).strftime("%Y-%m-%d %H:%M:%S"),
        "sunrise_utc":        datetime.utcfromtimestamp(
                                  record.get("sys", {}).get("sunrise", 0)
                              ).strftime("%Y-%m-%d %H:%M:%S"),
        "sunset_utc":         datetime.utcfromtimestamp(
                                  record.get("sys", {}).get("sunset", 0)
                              ).strftime("%Y-%m-%d %H:%M:%S"),

        # ── Weather Condition ──────────────────────────────────
        "weather_main":       record.get("weather", [{}])[0].get("main"),
        "weather_desc":       record.get("weather", [{}])[0].get("description"),
        "weather_id":         record.get("weather", [{}])[0].get("id"),

        # ── Temperature (already Celsius from API) ─────────────
        "temp_celsius":       record.get("main", {}).get("temp"),
        "feels_like_celsius": record.get("main", {}).get("feels_like"),
        "temp_min_celsius":   record.get("main", {}).get("temp_min"),
        "temp_max_celsius":   record.get("main", {}).get("temp_max"),

        # ── Atmosphere ─────────────────────────────────────────
        "humidity_pct":       record.get("main", {}).get("humidity"),
        "pressure_hpa":       record.get("main", {}).get("pressure"),
        "visibility_m":       record.get("visibility"),

        # ── Wind ───────────────────────────────────────────────
        "wind_speed_mps":     record.get("wind", {}).get("speed"),
        "wind_deg":           record.get("wind", {}).get("deg"),

        # ── Clouds & Rain ──────────────────────────────────────
        "cloud_pct":          record.get("clouds", {}).get("all"),
        "rain_1h_mm":         record.get("rain", {}).get("1h", 0.0),
    }


# ── Build DataFrame from a single raw file ────────────────────────────────────
def build_dataframe(wrapper: dict) -> pd.DataFrame:
    """
    Unpack the 'weather_data' list and flatten each city record.
    The wrapper structure (from the extractor) is:
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


# ── Clean Data ────────────────────────────────────────────────────────────────
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    initial = len(df)

    df = df.drop_duplicates()
    df = df.dropna(subset=["city", "timestamp_utc"])

    # Fields that are legitimately absent (no rain, unknown wind direction, etc.)
    for col in ["rain_1h_mm", "wind_deg", "visibility_m"]:
        df[col] = df[col].fillna(0)

    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"])
    df["humidity_pct"]  = df["humidity_pct"].astype(float)
    df["cloud_pct"]     = df["cloud_pct"].astype(float)

    logger.info(f"Clean: {initial} → {len(df)} rows (dropped {initial - len(df)})")
    return df


# ── Engineering Derived Columns ───────────────────────────────────────────────
def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Unit conversions and time features for analytics."""

    # Temperature
    df["temp_fahrenheit"] = ((df["temp_celsius"] * 9 / 5) + 32).round(2)

    # Wind
    df["wind_speed_kmh"] = (df["wind_speed_mps"] * 3.6).round(2)

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

    logger.info("Engineering columns added.")
    return df


# ── Change 3: Business / Analytics Columns ────────────────────────────────────

# Maps every OpenWeather 'main' value to one of 4 severity buckets
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

    temp_category    : Cold / Moderate / Hot       (based on temp_celsius)
    humidity_category: Low / Medium / High          (based on humidity_pct)
    weather_severity : Clear / Cloudy / Rainy / Storm (based on weather_main)
    """

    # Temperature Category  — <15 °C = Cold | 15–30 = Moderate | >30 = Hot
    df["temp_category"] = pd.cut(
        df["temp_celsius"],
        bins=[-float("inf"), 15, 30, float("inf")],
        labels=["Cold", "Moderate", "Hot"],
    ).astype(str)

    # Humidity Category  — <40% = Low | 40–70% = Medium | >70% = High
    df["humidity_category"] = pd.cut(
        df["humidity_pct"],
        bins=[-float("inf"), 40, 70, float("inf")],
        labels=["Low", "Medium", "High"],
    ).astype(str)

    # Weather Severity
    df["weather_severity"] = (
        df["weather_main"]
        .map(WEATHER_SEVERITY_MAP)
        .fillna("Clear")                # fallback for any unmapped value
    )

    logger.info("Business columns added.")
    return df


# ── Change 2: Save CSV — filename matches source JSON ─────────────────────────
def save_processed(df: pd.DataFrame, source_filename: str, output_folder: str) -> str:
    """
    Save the processed DataFrame as a CSV.
    Output name mirrors the source file for easy lineage tracking:

        IN  → data/raw/weather_20260807_103000.json
        OUT → data/processed/weather_20260807_103000.csv
    """
    os.makedirs(output_folder, exist_ok=True)
    base_name   = Path(source_filename).stem          # strip .json extension
    output_path = os.path.join(output_folder, f"{base_name}.csv")
    df.to_csv(output_path, index=False)
    logger.info(f"Saved: {output_path}")
    return output_path


# ── Change 4: Data Quality Report ─────────────────────────────────────────────
def print_quality_report(df: pd.DataFrame, filename: str) -> None:
    """Log and print a per-file data quality summary."""
    null_total = int(df.isnull().sum().sum())
    null_cols  = df.columns[df.isnull().any()].tolist()

    logger.info("--- Quality Report: %s ---", filename)
    logger.info("Rows Processed : %d", len(df))
    logger.info("Unique Cities  : %d", df["city"].nunique())
    logger.info("Null Values    : %d", null_total)
    if null_cols:
        logger.warning("Null Columns   : %s", null_cols)

    print(f"\n   📋 Quality Report")
    print(f"      Rows Processed  : {len(df)}")
    print(f"      Unique Cities   : {df['city'].nunique()}")
    print(f"      Null Values     : {null_total}")
    print(f"      Null Columns    : {null_cols if null_cols else 'None ✅'}")

    # Business column distribution (quick sanity check)
    print(f"      Temp Breakdown  : {df['temp_category'].value_counts().to_dict()}")
    print(f"      Severity Split  : {df['weather_severity'].value_counts().to_dict()}")


# ── Main ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("🔄  Starting transformation...")

    # ── Change 5: Load paths from config ──────────────────────
    try:
        config = load_config()
    except FileNotFoundError as e:
        print(f"❌  {e}")
        raise SystemExit(2)

    RAW_FOLDER       = config.get("raw_folder",       "data/raw")
    PROCESSED_FOLDER = config.get("processed_folder", "data/processed")
    METADATA_FILE    = config.get("metadata_file",    "data/processed_metadata/processed_files.json")

    # ── Change 1: Find only unprocessed files ─────────────────
    metadata  = load_metadata(METADATA_FILE)
    new_files = get_unprocessed_files(RAW_FOLDER, metadata)

    if not new_files:
        print("✅  No new files to process. Already up to date.")
        raise SystemExit(0)

    total_rows = 0

    for filename in new_files:
        filepath = os.path.join(RAW_FOLDER, filename)
        print(f"\n⚙️   Processing: {filename}")

        try:
            wrapper = load_raw_json(filepath)
            df      = build_dataframe(wrapper)

            if df.empty:
                logger.warning(f"No records in {filename} — skipping")
                print(f"   ⚠️  No weather_data records found — skipping")
                continue

            df = clean_dataframe(df)
            df = add_derived_columns(df)
            df = add_business_columns(df)         # Change 3

            output_path = save_processed(df, filename, PROCESSED_FOLDER)  # Change 2
            mark_as_processed(METADATA_FILE, filename)                     # Change 1

            total_rows += len(df)
            print(f"   ✅  Saved → {output_path}")

            print_quality_report(df, filename)                             # Change 4

        except Exception as e:
            logger.error(f"Failed to process {filename}: {e}")
            print(f"   ❌  Error: {e}")

    print(f"\n🏁  Done. Total rows written across all files: {total_rows}")