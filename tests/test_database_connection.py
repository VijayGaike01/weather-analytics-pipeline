import sqlite3
from pathlib import Path

from tests.test_logger import logger


def test_database_connection():

    logger.info("START : test_database_connection")

    db_path = Path("database/weather.db")

    assert db_path.exists(), "Database file does not exist."

    conn = sqlite3.connect(db_path)

    assert conn is not None

    conn.close()

    logger.info("PASS  : test_database_connection")