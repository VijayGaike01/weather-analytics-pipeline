-- ============================================================
-- ANALYTICAL QUERIES
-- ============================================================

-- Average temperature by city

SELECT
city,
ROUND(AVG(temp_celsius),2) AS avg_temperature
FROM weather_observations
GROUP BY city
ORDER BY avg_temperature DESC;

-- Highest temperature recorded

SELECT
city,
temp_celsius,
timestamp_utc
FROM weather_observations
ORDER BY temp_celsius DESC
LIMIT 1;

-- Lowest temperature recorded

SELECT
city,
temp_celsius,
timestamp_utc
FROM weather_observations
ORDER BY temp_celsius ASC
LIMIT 1;

-- Average humidity by city

SELECT
city,
ROUND(AVG(humidity_pct),2) AS avg_humidity
FROM weather_observations
GROUP BY city
ORDER BY avg_humidity DESC;

-- Weather condition distribution

SELECT
weather_main,
COUNT(*) AS record_count
FROM weather_observations
GROUP BY weather_main
ORDER BY record_count DESC;

-- Temperature category distribution

SELECT
temp_category,
COUNT(*) AS record_count
FROM weather_observations
GROUP BY temp_category;

-- Weather severity distribution

SELECT
weather_severity,
COUNT(*) AS record_count
FROM weather_observations
GROUP BY weather_severity;

-- Daily average temperature trend

SELECT
date,
ROUND(AVG(temp_celsius),2) AS avg_temperature
FROM weather_observations
GROUP BY date
ORDER BY date;

-- City-wise maximum temperature

SELECT
city,
MAX(temp_celsius) AS max_temperature
FROM weather_observations
GROUP BY city
ORDER BY max_temperature DESC;

-- City-wise rainfall summary

SELECT
city,
ROUND(SUM(rain_1h_mm),2) AS total_rainfall_mm
FROM weather_observations
GROUP BY city
ORDER BY total_rainfall_mm DESC;
