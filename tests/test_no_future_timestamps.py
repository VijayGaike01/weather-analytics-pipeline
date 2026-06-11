import sqlite3

from tests.test_logger import logger


def test_no_future_timestamps():

    logger.info("START : test_no_future_timestamps")

    conn = sqlite3.connect("database/weather.db")
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(*)
        FROM weather_observations
        WHERE timestamp_utc > CURRENT_TIMESTAMP
    """)

    invalid_count = cursor.fetchone()[0]

    conn.close()

    logger.info(
        f"INFO : future timestamp rows = {invalid_count}"
    )

    assert invalid_count == 0

    logger.info("PASS : test_no_future_timestamps")