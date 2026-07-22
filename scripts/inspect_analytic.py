import sqlite3
from pathlib import Path


DATABASE_PATH = Path("analytics.db")


if not DATABASE_PATH.exists():
    raise FileNotFoundError(
        f"Analytics database not found: {DATABASE_PATH.resolve()}"
    )


with sqlite3.connect(DATABASE_PATH) as connection:
    cursor = connection.cursor()

    tables = [
        "dim_customers",
        "fct_orders",
    ]

    for table_name in tables:
        row_count = cursor.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()[0]

        print(f"{table_name}: {row_count} rows")

        rows = cursor.execute(
            f"SELECT * FROM {table_name} LIMIT 5"
        ).fetchall()

        for row in rows:
            print(row)

        print()

with sqlite3.connect("analytics.db") as connection:
    rows = connection.execute(
        """
        SELECT
            order_id,
            order_date,
            total_amount,
            currency,
            usd_amount
        FROM fct_orders
        ORDER BY order_id
        """
    ).fetchall()

    print("check currency conversion")
    for row in rows:
        print(row)