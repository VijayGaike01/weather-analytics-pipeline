import json
import os
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path

# ── Logging Setup ─────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/transform.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# ── 1. Read Raw JSON ──────────────────────────────────────────
def load_raw_json(filepath: str) -> dict:
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    logger.info(f"Loaded: {filepath}")
    return data


# ── 2. Flatten ONE city record ────────────────────────────────
def flatten_weather(record: dict) -> dict:
    """
    Flattens a single city dict from the 'weather_data' list.
    Temps are already Celsius because your extractor uses units=metric.
    """
    return {
        # Identity
        "city":               record.get("name"),
        "country":            record.get("sys", {}).get("country"),
        "latitude":           record.get("coord", {}).get("lat"),
        "longitude":          record.get("coord", {}).get("lon"),

        # Time
        "timestamp_utc":      datetime.utcfromtimestamp(record.get("dt", 0)).strftime("%Y-%m-%d %H:%M:%S"),
        "sunrise_utc":        datetime.utcfromtimestamp(record.get("sys", {}).get("sunrise", 0)).strftime("%Y-%m-%d %H:%M:%S"),
        "sunset_utc":         datetime.utcfromtimestamp(record.get("sys", {}).get("sunset", 0)).strftime("%Y-%m-%d %H:%M:%S"),

        # Weather condition
        "weather_main":       record.get("weather", [{}])[0].get("main"),
        "weather_desc":       record.get("weather", [{}])[0].get("description"),
        "weather_id":         record.get("weather", [{}])[0].get("id"),

        # Temperature — already Celsius (units=metric in your extractor)
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

        # Clouds & Rain
        "cloud_pct":          record.get("clouds", {}).get("all"),
        "rain_1h_mm":         record.get("rain", {}).get("1h", 0.0),
    }


# ── 3. Build DataFrame ────────────────────────────────────────
def build_dataframe(raw_folder: str) -> pd.DataFrame:
    """
    Your extractor saves one JSON file per run, structured as:
    {
        "ingestion_timestamp": "...",
        "weather_data": [ {city1}, {city2}, ... ],  ← the actual records
        "failed_cities": [...]
    }
    We must unpack 'weather_data', not pass the wrapper to flatten_weather.
    """
    rows = []
    files = [f for f in os.listdir(raw_folder) if f.endswith(".json")]

    if not files:
        logger.warning(f"No JSON files found in {raw_folder}")
        return pd.DataFrame()

    for filename in sorted(files):  # sorted = chronological order
        filepath = os.path.join(raw_folder, filename)
        try:
            wrapper = load_raw_json(filepath)

            # ← THIS was the bug: old code passed `wrapper` directly
            #   to flatten_weather. The cities live inside weather_data.
            city_records = wrapper.get("weather_data", [])

            if not city_records:
                logger.warning(f"No weather_data records in {filename} — skipping")
                continue

            for record in city_records:
                row = flatten_weather(record)
                row["ingestion_timestamp"] = wrapper.get("ingestion_timestamp")
                rows.append(row)

            logger.info(f"{filename} → {len(city_records)} city records extracted")

        except Exception as e:
            logger.error(f"Failed to process {filename}: {e}")

    df = pd.DataFrame(rows)
    logger.info(f"DataFrame built: {len(df)} rows from {len(files)} files")
    return df


# ── 4. Clean Data ─────────────────────────────────────────────
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    initial = len(df)

    df = df.drop_duplicates()
    df = df.dropna(subset=["city", "timestamp_utc"])

    # Fill expected-missing numerics with 0
    for col in ["rain_1h_mm", "wind_deg", "visibility_m"]:
        df[col] = df[col].fillna(0)

    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"])
    df["humidity_pct"]  = df["humidity_pct"].astype(float)
    df["cloud_pct"]     = df["cloud_pct"].astype(float)

    logger.info(f"Cleaned: {initial} → {len(df)} rows (dropped {initial - len(df)})")
    return df


# ── 5. Derived Columns ────────────────────────────────────────
def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:

    # Fahrenheit
    df["temp_fahrenheit"] = ((df["temp_celsius"] * 9 / 5) + 32).round(2)

    # Wind: m/s → km/h
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

    # Daylight duration in hours
    df["sunrise_utc"]  = pd.to_datetime(df["sunrise_utc"])
    df["sunset_utc"]   = pd.to_datetime(df["sunset_utc"])
    df["daylight_hrs"] = (
        (df["sunset_utc"] - df["sunrise_utc"]).dt.seconds / 3600
    ).round(2)

    logger.info("Derived columns added.")
    return df


# ── 6. Save to CSV ────────────────────────────────────────────
def save_processed(df: pd.DataFrame, output_folder: str) -> str:
    os.makedirs(output_folder, exist_ok=True)
    ts       = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(output_folder, f"weather_processed_{ts}.csv")
    df.to_csv(filepath, index=False)
    logger.info(f"Saved: {filepath}")
    print(f"✅  Saved → {filepath}")
    return filepath


# ── Main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    RAW_FOLDER       = "data/raw"
    PROCESSED_FOLDER = "data/processed"

    print("🔄  Starting transformation...")

    df = build_dataframe(RAW_FOLDER)

    if df.empty:
        print("⚠️   No data found. Run the extractor first.")
    else:
        df = clean_dataframe(df)
        df = add_derived_columns(df)
        save_processed(df, PROCESSED_FOLDER)

        print(f"\n📊  Summary")
        print(f"    Rows    : {len(df)}")
        print(f"    Columns : {len(df.columns)}")
        print(f"    Cities  : {df['city'].unique().tolist()}")
        print(f"\n🌡️   Sample:")
        print(
            df[["city", "timestamp_utc", "temp_celsius",
                "humidity_pct", "weather_desc"]]
            .to_string(index=False)
        )