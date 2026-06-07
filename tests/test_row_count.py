import sqlite3

from tests.test_logger import logger


def test_weather_table_has_rows():

    logger.info("START : test_weather_table_has_rows")

    conn = sqlite3.connect("database/weather.db")

    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(*)
        FROM weather_observations
    """)

    row_count = cursor.fetchone()[0]

    conn.close()

    logger.info(
        f"INFO  : weather_observations row count = {row_count}"
    )

    assert row_count > 0

    logger.info("PASS  : test_weather_table_has_rows")