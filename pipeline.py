from pathlib import Path

from prefect import flow, get_run_logger

from etl.config import SOURCE_DATABASE_PATH
from etl.extract import extract_source_data
from etl.transform import transform_customers, transform_orders

@flow(name="shopdata-etl-flow")
def shopdata_etl_flow(
    database_path: str = str(SOURCE_DATABASE_PATH),
) -> None:
    """
    Run the current ShopData ETL stages.

    Current stages:
    1. Extract all source views.
    2. Clean customer data.
    """
    logger = get_run_logger()

    logger.info("Starting ShopData ETL pipeline")

    customers, orders, exchange_rates = extract_source_data(
        Path(database_path)
    )

    cleaned_customers = transform_customers(customers)

    cleaned_orders = transform_orders(
        orders,
        exchange_rates,
    )

    # Temporary validation logs until the Load stage is implemented.
    logger.info(
        "Cleaned customer rows: %d",
        len(cleaned_customers),
    )

    logger.info(
        "Cleaned order rows: %d",
        len(cleaned_orders),
    )

    logger.info(
        "Cleaned order columns: %s",
        cleaned_orders.columns.tolist(),
    )

    logger.info("Current ETL stages completed successfully")


if __name__ == "__main__":
    shopdata_etl_flow()