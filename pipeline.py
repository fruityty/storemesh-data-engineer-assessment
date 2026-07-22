from pathlib import Path

from prefect import flow, get_run_logger

from etl.config import SOURCE_DATABASE_PATH
from etl.extract import extract_source_data


@flow(name="shopdata-extract-flow")
def extract_flow(
    database_path: str = str(SOURCE_DATABASE_PATH),
) -> None:
    """
    Orchestrate the extraction stage of the ShopData ETL pipeline.
    """
    logger = get_run_logger()

    logger.info("Starting source-data extraction")

    # Prefect records this function call as a separate task run.
    customers, orders, exchange_rates = extract_source_data(
        Path(database_path)
    )

    # Temporary schema checks confirm that extraction returned the
    # expected columns before transformation logic is added.
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

    logger.info("Source-data extraction completed successfully")


if __name__ == "__main__":
    # Run the flow only when this file is executed directly.
    extract_flow()