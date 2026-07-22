from pathlib import Path

from prefect import flow, get_run_logger

from etl.config import ANALYTICS_DATABASE_PATH, SOURCE_DATABASE_PATH
from etl.extract import extract_source_data
from etl.load import load_analytics_data
from etl.transform import transform_customers, transform_orders


@flow(name="shopdata-etl-flow")
def shopdata_etl_flow(
    source_database: str = str(SOURCE_DATABASE_PATH),
    analytics_database: str = str(ANALYTICS_DATABASE_PATH),
) -> None:
    """
    Run the complete ShopData ETL pipeline.

    Stages:
    1. Extract source data.
    2. Clean customer and order data.
    3. Load cleaned data into analytics.db.
    """
    logger = get_run_logger()

    logger.info("Starting ShopData ETL pipeline")

    customers, orders, exchange_rates = extract_source_data(
        Path(source_database)
    )

    cleaned_customers = transform_customers(customers)

    cleaned_orders = transform_orders(
        orders,
        exchange_rates,
    )

    load_analytics_data(
        cleaned_customers,
        cleaned_orders,
        Path(analytics_database),
    )

    logger.info("ShopData ETL pipeline completed successfully")


if __name__ == "__main__":
    shopdata_etl_flow()