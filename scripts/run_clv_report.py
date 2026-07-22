import sqlite3
from pathlib import Path

import pandas as pd


DATABASE_PATH = Path("analytics.db")
SQL_PATH = Path("sql_scripts/clv_report.sql")


def run_clv_report() -> None:
    """Execute the CLV query and display the result."""
    if not DATABASE_PATH.exists():
        raise FileNotFoundError(
            f"Analytics database not found: {DATABASE_PATH.resolve()}"
        )

    if not SQL_PATH.exists():
        raise FileNotFoundError(
            f"CLV SQL file not found: {SQL_PATH.resolve()}"
        )

    query = SQL_PATH.read_text(encoding="utf-8")

    with sqlite3.connect(DATABASE_PATH) as connection:
        report = pd.read_sql_query(query, connection)

    print(report.to_string(index=False))


if __name__ == "__main__":
    run_clv_report()