from pathlib import Path

from prefect import flow, get_run_logger

from etl.config import SOURCE_DATABASE_PATH
from etl.extract import extract_source_data
from etl.transform import transform_customers


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


    # Temporary validation logs until the Load stage is implemented.
    logger.info(
        "Cleaned customer columns: %s",
        cleaned_customers.columns.tolist(),
    )

    logger.info(
        "Cleaned customer IDs: %s",
        cleaned_customers["customer_id"].tolist(),
    )

    # These extracted datasets will be used in the next order-transform step.
    logger.info(
        "Orders ready for transformation: %d",
        len(orders),
    )

    logger.info(
        "Exchange rates ready for transformation: %d",
        len(exchange_rates),
    )

    logger.info("Current ETL stages completed successfully")


if __name__ == "__main__":
    shopdata_etl_flow()