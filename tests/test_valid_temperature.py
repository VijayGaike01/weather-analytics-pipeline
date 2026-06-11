import sqlite3

from tests.test_logger import logger


def test_valid_temperature_range():

    logger.info("START : test_valid_temperature_range")

    conn = sqlite3.connect("database/weather.db")
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(*)
        FROM weather_observations
        WHERE temperature < -100
           OR temperature > 70
    """)

    invalid_count = cursor.fetchone()[0]

    conn.close()

    logger.info(
        f"INFO : invalid temperature rows = {invalid_count}"
    )

    assert invalid_count == 0

    logger.info("PASS : test_valid_temperature_range")