import sqlite3

from tests.test_logger import logger


def test_valid_coordinates():

    logger.info("START : test_valid_coordinates")

    conn = sqlite3.connect("database/weather.db")
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(*)
        FROM weather_observations
        WHERE latitude < -90
           OR latitude > 90
           OR longitude < -180
           OR longitude > 180
    """)

    invalid_count = cursor.fetchone()[0]

    conn.close()

    logger.info(
        f"INFO : invalid coordinate rows = {invalid_count}"
    )

    assert invalid_count == 0

    logger.info("PASS : test_valid_coordinates")