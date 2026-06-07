import sqlite3

from tests.test_logger import logger


def test_weather_observations_table_exists():

    logger.info("START : test_weather_observations_table_exists")

    conn = sqlite3.connect("database/weather.db")

    cursor = conn.cursor()

    cursor.execute("""
        SELECT name
        FROM sqlite_master
        WHERE type='table'
        AND name='weather_observations'
    """)

    result = cursor.fetchone()

    conn.close()

    assert result is not None

    logger.info("PASS  : test_weather_observations_table_exists")


def test_etl_load_audit_table_exists():

    logger.info("START : test_etl_load_audit_table_exists")

    conn = sqlite3.connect("database/weather.db")

    cursor = conn.cursor()

    cursor.execute("""
        SELECT name
        FROM sqlite_master
        WHERE type='table'
        AND name='etl_load_audit'
    """)

    result = cursor.fetchone()

    conn.close()

    assert result is not None

    logger.info("PASS  : test_etl_load_audit_table_exists")