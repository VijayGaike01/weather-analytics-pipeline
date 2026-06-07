-- ============================================================
-- Weather Analytics Pipeline
-- Phase 3 : Database Layer
-- Database : SQLite
-- ============================================================

-- ============================================================
-- WEATHER OBSERVATIONS TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS weather_observations (

    observation_id INTEGER PRIMARY KEY AUTOINCREMENT,

    city TEXT NOT NULL,
    country TEXT,

    latitude REAL,
    longitude REAL,

    timestamp_utc TEXT NOT NULL,
    sunrise_utc TEXT,
    sunset_utc TEXT,

    weather_main TEXT,
    weather_desc TEXT,
    weather_id INTEGER,

    temp_celsius REAL,
    feels_like_celsius REAL,
    temp_min_celsius REAL,
    temp_max_celsius REAL,

    humidity_pct REAL,
    pressure_hpa REAL,
    visibility_m REAL,

    wind_speed_mps REAL,
    wind_deg REAL,

    cloud_pct REAL,
    rain_1h_mm REAL,

    ingestion_timestamp TEXT NOT NULL,

    temp_fahrenheit REAL,
    wind_speed_kmh REAL,
    heat_index REAL,

    date TEXT,
    hour INTEGER,
    day_of_week TEXT,

    daylight_hrs REAL,

    temp_category TEXT,
    humidity_category TEXT,
    weather_severity TEXT,

    record_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

-- ============================================================
-- ETL LOAD AUDIT TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS etl_load_audit (

    load_id INTEGER PRIMARY KEY AUTOINCREMENT,

    source_file TEXT NOT NULL,

    rows_loaded INTEGER,

    load_status TEXT,

    error_message TEXT,

    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

-- ============================================================
-- INDEXES
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_weather_city
ON weather_observations(city);

CREATE INDEX IF NOT EXISTS idx_weather_timestamp
ON weather_observations(timestamp_utc);

CREATE INDEX IF NOT EXISTS idx_weather_ingestion
ON weather_observations(ingestion_timestamp);

CREATE INDEX IF NOT EXISTS idx_weather_main
ON weather_observations(weather_main);

CREATE INDEX IF NOT EXISTS idx_weather_date
ON weather_observations(date);