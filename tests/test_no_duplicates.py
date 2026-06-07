import sqlite3

from tests.test_logger import logger


def test_no_duplicate_records():

    logger.info("START : test_no_duplicate_records")

    conn = sqlite3.connect("database/weather.db")

    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT
                city,
                timestamp_utc,
                ingestion_timestamp,
                COUNT(*) AS cnt
            FROM weather_observations
            GROUP BY
                city,
                timestamp_utc,
                ingestion_timestamp
            HAVING COUNT(*) > 1
        )
    """)

    duplicate_count = cursor.fetchone()[0]

    conn.close()

    logger.info(
        f"INFO  : duplicate count = {duplicate_count}"
    )

    assert duplicate_count == 0

    logger.info("PASS  : test_no_duplicate_records")