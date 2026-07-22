import sqlite3
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect.logging import get_run_logger

# Default paths
SOURCE_DATABASE_PATH = Path("data/shopdata.db")


@task(
    name="extract-source-data",
    retries=2,
    retry_delay_seconds=2,
)
def extract_source_data(database_path: Path,) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Extract customers, orders, and exchange rates from SQLite views.

    The source database is opened in read-only mode to prevent accidental
    modifications.
    """
    logger = get_run_logger()

    # sqlite3.connect() can create an empty database when the path is wrong.
    # validate the path before connecting.
    if not database_path.exists():
        raise FileNotFoundError(
            f"Source database not found: {database_path.resolve()}"
        )

    try:
        logger.info(
            "Connecting to source database: %s",
            database_path.resolve(),
        )

        # Open the provided SQLite database in read-only mode.
        database_uri = f"{database_path.resolve().as_uri()}?mode=ro"

        with sqlite3.connect(database_uri, uri=True) as connection:
            customers = pd.read_sql_query(
                """
                SELECT
                    customer_id,
                    full_name,
                    email,
                    phone,
                    signup_date
                FROM vw_raw_customers
                """,
                connection,
            )

            orders = pd.read_sql_query(
                """
                SELECT
                    order_id,
                    customer_id,
                    order_date,
                    total_amount,
                    currency,
                    status
                FROM vw_raw_orders
                """,
                connection,
            )

            exchange_rates = pd.read_sql_query(
                """
                SELECT
                    currency,
                    rate_to_usd,
                    date
                FROM vw_exchange_rates
                """,
                connection,
            )

        logger.info(
            "Extraction completed: customers=%d, orders=%d, exchange_rates=%d",
            len(customers),
            len(orders),
            len(exchange_rates),
        )

        return customers, orders, exchange_rates

    except (sqlite3.Error, pd.errors.DatabaseError):
        logger.exception(
            "Failed to extract data from %s",
            database_path,
        )
        raise


@flow(name="shopdata-extract-flow")
def extract_flow(database_path: str = str(SOURCE_DATABASE_PATH),) -> None:
    """Run and verify the extraction stage."""
    logger = get_run_logger()

    logger.info("Starting source-data extraction")

    customers, orders, exchange_rates = extract_source_data(
        Path(database_path)
    )

    # Temporary preview for development and verification.
    logger.info(
        "Customer columns: %s",
        customers.columns.tolist(),
    )
    logger.info(
        "Order columns: %s",
        orders.columns.tolist(),
    )
    logger.info(
        "Exchange-rate columns: %s",
        exchange_rates.columns.tolist(),
    )

    logger.info("Source-data extraction finished successfully")


if __name__ == "__main__":
    extract_flow()