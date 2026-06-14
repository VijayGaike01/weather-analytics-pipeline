"""
export_powerbi_dataset.py
=========================

Exports weather observations from SQLite
into a Power BI friendly CSV dataset.

Output:
    powerbi/weather_dataset.csv
"""

from pathlib import Path
import sqlite3
import pandas as pd

from pipeline_config import DEFAULTS, get_logger


logger = get_logger(
    name="powerbi_export",
    log_file="powerbi_export.log"
)


def export_dataset() -> str:

    logger.info("START : Power BI dataset export")

    db_path = Path(DEFAULTS.database_file)

    if not db_path.exists():
        raise FileNotFoundError(
            f"Database not found: {db_path}"
        )

    output_dir = Path("powerbi")
    output_dir.mkdir(exist_ok=True)

    output_file = output_dir / "weather_dataset.csv"

    conn = sqlite3.connect(db_path)

    query = """
        SELECT *
        FROM weather_observations
        ORDER BY timestamp_utc DESC
    """

    df = pd.read_sql_query(query, conn)

    conn.close()

    df.to_csv(
        output_file,
        index=False,
        encoding="utf-8"
    )

    logger.info(
        "SUCCESS : Exported %s rows to %s",
        len(df),
        output_file
    )

    return str(output_file)


if __name__ == "__main__":
    export_dataset()