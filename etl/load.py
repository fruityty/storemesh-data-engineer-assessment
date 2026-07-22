import sqlite3
from pathlib import Path

import pandas as pd
from prefect import get_run_logger, task


def write_analytics_data(
    customers: pd.DataFrame,
    orders: pd.DataFrame,
    database_path: Path,
) -> None:
    """
    Write cleaned customer and order data to an SQLite database.

    Existing analytical tables are replaced so the pipeline can be
    executed repeatedly without creating duplicate records.
    """

    # Create the parent directory if a nested output path is provided.
    database_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with sqlite3.connect(database_path) as connection:
        customers.to_sql(
            name="dim_customers",
            con=connection,
            if_exists="replace",
            index=False,
        )

        orders.to_sql(
            name="fct_orders",
            con=connection,
            if_exists="replace",
            index=False,
        )

        # Customer IDs should be unique after deduplication.
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS
                idx_dim_customers_customer_id
            ON dim_customers(customer_id)
            """
        )

        # Improve customer-based analytical joins.
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
                idx_fct_orders_customer_id
            ON fct_orders(customer_id)
            """
        )

        # Improve date-based analytical queries.
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS
                idx_fct_orders_order_date
            ON fct_orders(order_date)
            """
        )


@task(
    name="load-analytics-data",
    retries=2,
    retry_delay_seconds=2,
)
def load_analytics_data(
    customers: pd.DataFrame,
    orders: pd.DataFrame,
    database_path: Path,
) -> None:
    """Prefect task that loads cleaned data into analytics.db."""
    logger = get_run_logger()

    try:
        logger.info(
            "Loading analytical data into %s",
            database_path.resolve(),
        )

        write_analytics_data(
            customers,
            orders,
            database_path,
        )

        logger.info(
            "Load completed: dim_customers=%d rows, fct_orders=%d rows",
            len(customers),
            len(orders),
        )

    except (sqlite3.Error, ValueError, OSError):
        logger.exception(
            "Failed to load analytical data into %s",
            database_path,
        )
        raise