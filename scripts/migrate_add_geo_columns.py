"""
migrate_add_geo_columns.py
===========================
One-off migration for an EXISTING weather.db: adds the district / division /
region columns to the weather_observations table.

Why this is needed: the live extract now carries Maharashtra's tehsil ->
district -> division -> region hierarchy through from
maharashtra_tehsils_final.csv, and transform.py's flatten_weather() emits
those as columns. But if weather.db already exists from before this change,
pandas.to_sql(..., if_exists="append") will fail with "table
weather_observations has no column named division" the next time load_weather
runs, because the table itself doesn't have these columns yet.

This script adds them in place. Idempotent — safe to run more than once,
and safe to run even if some/all columns already exist.

Run once locally, from the project root:
    python migrate_add_geo_columns.py

Then also add the same three columns to sql/create_tables.sql's
CREATE TABLE weather_observations (...) statement, so a FRESH database
(e.g. a clean GitHub Actions checkout without a committed weather.db)
gets them too:

    district TEXT,
    division TEXT,
    region   TEXT,
"""

import sqlite3
from pathlib import Path

from pipeline_config import DEFAULTS

NEW_COLUMNS: dict[str, str] = {
    "district": "TEXT",
    "division": "TEXT",
    "region":   "TEXT",
}

TABLE_NAME = "weather_observations"


def migrate(db_path: str = DEFAULTS.database_file) -> None:
    path = Path(db_path)
    if not path.exists():
        print(f"No database found at {path} — nothing to migrate. "
              f"(A fresh load_weather.py run will create the table from "
              f"sql/create_tables.sql, so make sure that script has the new "
              f"columns too — see the docstring at the top of this file.)")
        return

    conn = sqlite3.connect(path)
    try:
        existing_cols = {row[1] for row in conn.execute(f"PRAGMA table_info({TABLE_NAME})")}

        if not existing_cols:
            print(f"Table '{TABLE_NAME}' doesn't exist in {path} yet — nothing to migrate.")
            return

        added = []
        for col, col_type in NEW_COLUMNS.items():
            if col in existing_cols:
                print(f"  - '{col}' already exists, skipping.")
                continue
            conn.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN {col} {col_type}")
            added.append(col)
            print(f"  + added '{col}' ({col_type})")

        conn.commit()
    finally:
        conn.close()

    if added:
        print(f"\nMigration complete — added {added} to {path}.")
    else:
        print(f"\nMigration complete — {path} already had all columns.")


if __name__ == "__main__":
    migrate()