import sqlite3
from pathlib import Path

# Path to the source SQLite database provided for the assessment.
DATABASE_PATH = Path("data/shopdata.db")

# SQL script containing data quality exploration queries.
SQL_PATH = Path("sql_scripts/exploration.sql")

# Maximum number of result rows displayed for each query.
MAX_DISPLAY_ROWS = 10


def run_exploration() -> None:
    """
    Execute each SQL statement in exploration.sql and display compact results.

    Each query displays its column names and up to MAX_DISPLAY_ROWS records.
    """

    # Validate that the source database exists.
    if not DATABASE_PATH.exists():
        raise FileNotFoundError(
            f"Database not found: {DATABASE_PATH.resolve()}"
        )

    # Validate that the SQL script exists.
    if not SQL_PATH.exists():
        raise FileNotFoundError(
            f"SQL file not found: {SQL_PATH.resolve()}"
        )

    # Read the SQL script into memory.
    sql_text = SQL_PATH.read_text(encoding="utf-8")

    # Split the script into individual SQL statements.
    statements = [
        statement.strip()
        for statement in sql_text.split(";")
        if statement.strip()
    ]

    # Connect to the SQLite database.
    with sqlite3.connect(DATABASE_PATH) as connection:
        cursor = connection.cursor()

        for statement_number, statement in enumerate(statements, start=1):
            print(f"\n{'=' * 50}")
            print(f"QUERY {statement_number}")
            print("=" * 50)

            try:
                cursor.execute(statement)

                # SELECT and PRAGMA statements return rows.
                if cursor.description is None:
                    print("Query completed. No rows returned.")
                    continue

                column_names = [
                    column[0] for column in cursor.description
                ]

                print("Columns:", " | ".join(column_names))

                # Fetch one extra row to determine whether results were truncated.
                rows = cursor.fetchmany(MAX_DISPLAY_ROWS + 1)
                displayed_rows = rows[:MAX_DISPLAY_ROWS]

                if not displayed_rows:
                    print("No matching records found.")
                    continue

                for row in displayed_rows:
                    print(row)

                if len(rows) > MAX_DISPLAY_ROWS:
                    print(
                        f"... more rows not shown "
                        f"(display limit: {MAX_DISPLAY_ROWS})"
                    )

            except sqlite3.Error as error:
                print(f"SQL error: {error}")


if __name__ == "__main__":
    run_exploration()
