import sqlite3

from tests.test_logger import logger


def test_no_null_cities():

    logger.info("START : test_no_null_cities")

    conn = sqlite3.connect("database/weather.db")

    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(*)
        FROM weather_observations
        WHERE city IS NULL
    """)

    null_count = cursor.fetchone()[0]

    conn.close()

    logger.info(
        f"INFO  : null city count = {null_count}"
    )

    assert null_count == 0

    logger.info("PASS  : test_no_null_cities")