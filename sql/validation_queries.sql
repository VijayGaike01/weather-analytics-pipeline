-- ============================================================
-- DATA LOAD VALIDATION QUERIES
-- ============================================================

-- Total records loaded

SELECT COUNT(*) AS total_records
FROM weather_observations;

-- Distinct cities loaded

SELECT COUNT(DISTINCT city) AS distinct_cities
FROM weather_observations;

-- Date range available

SELECT
MIN(timestamp_utc) AS earliest_record,
MAX(timestamp_utc) AS latest_record
FROM weather_observations;

-- Check for duplicate observations

SELECT
city,
timestamp_utc,
COUNT(*) AS duplicate_count
FROM weather_observations
GROUP BY city, timestamp_utc
HAVING COUNT(*) > 1;

-- Null city check

SELECT COUNT(*) AS null_city_records
FROM weather_observations
WHERE city IS NULL;

-- Null temperature check

SELECT COUNT(*) AS null_temperature_records
FROM weather_observations
WHERE temp_celsius IS NULL;

-- ETL Load Audit Summary

SELECT *
FROM etl_load_audit
ORDER BY load_timestamp DESC;
