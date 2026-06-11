import sqlite3

from tests.test_logger import logger


def test_valid_humidity_range():

    logger.info("START : test_valid_humidity_range")

    conn = sqlite3.connect("database/weather.db")
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(*)
        FROM weather_observations
        WHERE humidity < 0
           OR humidity > 100
    """)

    invalid_count = cursor.fetchone()[0]

    conn.close()

    logger.info(
        f"INFO : invalid humidity rows = {invalid_count}"
    )

    assert invalid_count == 0

    logger.info("PASS : test_valid_humidity_range")