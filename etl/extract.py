import sqlite3
from pathlib import Path
import pandas as pd
from prefect import get_run_logger, task

# Explicit columns make the expected source schema clear and prevent
# unexpected columns from entering the pipeline.
CUSTOMERS_QUERY = """
SELECT
    customer_id,
    full_name,
    email,
    phone,
    signup_date
FROM vw_raw_customers
"""

ORDERS_QUERY = """
SELECT
    order_id,
    customer_id,
    order_date,
    total_amount,
    currency,
    status
FROM vw_raw_orders
"""

EXCHANGE_RATES_QUERY = """
SELECT
    currency,
    rate_to_usd,
    date
FROM vw_exchange_rates
"""

def read_source_data(database_path: Path,) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Read customers, orders, and exchange rates from the source database.

    This function contains only the database extraction logic, allowing it
    to be tested independently from Prefect.
    """

    # SQLite can create a new empty database when a path is incorrect,
    # so validate the source file before opening it.
    if not database_path.exists():
        raise FileNotFoundError(
            f"Source database not found: {database_path.resolve()}"
        )

    # Open the supplied database in read-only mode to prevent accidental
    # changes to the original assessment data.
    database_uri = f"{database_path.resolve().as_uri()}?mode=ro"

    with sqlite3.connect(database_uri, uri=True) as connection:
        customers = pd.read_sql_query(
            CUSTOMERS_QUERY,
            connection,
        )

        orders = pd.read_sql_query(
            ORDERS_QUERY,
            connection,
        )

        exchange_rates = pd.read_sql_query(
            EXCHANGE_RATES_QUERY,
            connection,
        )

    return customers, orders, exchange_rates

@task(
    name="extract-source-data",
    retries=2,
    retry_delay_seconds=2,
)
def extract_source_data(database_path: Path,) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Prefect task that extracts all required source datasets.

    The task wrapper adds retries, Prefect logging, and run-state tracking
    around the underlying read_source_data function.
    """
    logger = get_run_logger()

    try:
        logger.info(
            "Extracting data from %s",
            database_path.resolve(),
        )

        customers, orders, exchange_rates = read_source_data(
            database_path
        )

        # Record row counts so the pipeline run can be audited.
        logger.info(
            "Extraction completed: customers=%d, orders=%d, "
            "exchange_rates=%d",
            len(customers),
            len(orders),
            len(exchange_rates),
        )

        return customers, orders, exchange_rates

    except (
        FileNotFoundError,
        sqlite3.Error,
        pd.errors.DatabaseError,
    ):
        # logger.exception includes the stack trace in Prefect logs.
        logger.exception(
            "Source-data extraction failed for %s",
            database_path,
        )
        raise