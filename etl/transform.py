import re
import pandas as pd
from prefect import get_run_logger, task


CUSTOMER_REQUIRED_COLUMNS = {
    "customer_id",
    "full_name",
    "email",
    "phone",
    "signup_date",
}

ORDER_REQUIRED_COLUMNS = {
    "order_id",
    "customer_id",
    "order_date",
    "total_amount",
    "currency",
    "status",
}

EXCHANGE_RATE_REQUIRED_COLUMNS = {
    "currency",
    "rate_to_usd",
    "date",
}

def validate_required_columns(
    dataframe: pd.DataFrame,
    required_columns: set[str],
    dataframe_name: str,
) -> None:
    """Raise an error when expected source columns are missing."""
    missing_columns = required_columns - set(dataframe.columns)

    if missing_columns:
        missing_text = ", ".join(sorted(missing_columns))
        raise ValueError(
            f"{dataframe_name} is missing required columns: {missing_text}"
        )


def standardize_phone(phone: object) -> str | None:
    """
    Remove all non-numeric characters from a phone number.

    Examples:
        +1 (555) 123-4567 -> 15551234567
        555-987-6543      -> 5559876543
        Ext 444           -> 444
    """
    if pd.isna(phone):
        return None

    # Remove all non-digits
    cleaned_phone = re.sub(r"\D", "", str(phone))

    # Return None if the original value contained no digits.
    return cleaned_phone or None


def clean_customers(customers: pd.DataFrame) -> pd.DataFrame:
    """
    Apply customer cleaning rules.

    Rules:
    1. Deduplicate using customer_id.
    2. Keep the row with the most recent signup_date.
    3. Remove non-numeric characters from phone.
    4. Replace missing email with unknown@domain.com.
    """
    validate_required_columns(
        customers,
        CUSTOMER_REQUIRED_COLUMNS,
        "Customer data",
    )

    # Avoid modifying the extracted DataFrame directly.
    cleaned = customers.copy()

    # Convert signup_date into datetime for reliable chronological sorting.
    cleaned["_parsed_signup_date"] = pd.to_datetime(
        cleaned["signup_date"],
        errors="coerce",
    )

    # Sort newest records first, then retain one record per customer.
    cleaned = (
        cleaned.sort_values(
            by=["customer_id", "_parsed_signup_date"],
            ascending=[True, False],
            na_position="last",
            kind="stable",
        )
        .drop_duplicates(
            subset=["customer_id"],
            keep="first",
        )
        .reset_index(drop=True)
    )

    # Apply phone cleaning after duplicate rows have been removed.
    cleaned["phone"] = cleaned["phone"].apply(standardize_phone)

    # Treat NULL, empty strings, and whitespace-only values as missing.
    cleaned["email"] = (
        cleaned["email"]
        .astype("string")
        .str.strip()
        .replace("", pd.NA)
        .fillna("unknown@domain.com")
    )

    # Store valid dates in a consistent SQLite-friendly format.
    cleaned["signup_date"] = (
        cleaned["_parsed_signup_date"]
        .dt.strftime("%Y-%m-%d")
    )

    # Remove the temporary column used for sorting.
    cleaned = cleaned.drop(columns=["_parsed_signup_date"])

    return cleaned

def clean_orders(
    orders: pd.DataFrame,
    exchange_rates: pd.DataFrame,
) -> pd.DataFrame:
    """
    Apply order cleaning and currency-conversion rules.

    Rules:
    1. Remove orders with total_amount less than or equal to zero.
    2. Treat missing or blank currencies as USD.
    3. Match exchange rates using currency and order_date.
    4. Convert matched non-USD amounts using rate_to_usd.
    5. Treat orders without a matching rate as already being in USD.
    """
    validate_required_columns(
        orders,
        ORDER_REQUIRED_COLUMNS,
        "Order data",
    )

    validate_required_columns(
        exchange_rates,
        EXCHANGE_RATE_REQUIRED_COLUMNS,
        "Exchange-rate data",
    )

    # Avoid modifying the extracted DataFrames directly.
    cleaned_orders = orders.copy()
    cleaned_rates = exchange_rates.copy()

    # Convert amounts to numeric. Invalid values become NaN.
    cleaned_orders["total_amount"] = pd.to_numeric(
        cleaned_orders["total_amount"],
        errors="coerce",
    )

    # Remove missing, zero, and negative amounts.
    cleaned_orders = cleaned_orders.loc[
        cleaned_orders["total_amount"].notna()
        & (cleaned_orders["total_amount"] > 0)
    ].copy()

    # Normalize currency values and treat missing currencies as USD.
    cleaned_orders["currency"] = (
        cleaned_orders["currency"]
        .astype("string")
        .str.strip()
        .str.upper()
        .replace("", pd.NA)
        .fillna("USD")
    )

    # Parse dates so orders can be matched to daily exchange rates.
    cleaned_orders["_order_date_key"] = pd.to_datetime(
        cleaned_orders["order_date"],
        errors="coerce",
    ).dt.normalize()

    # Normalize exchange-rate columns before joining.
    cleaned_rates["currency"] = (
        cleaned_rates["currency"]
        .astype("string")
        .str.strip()
        .str.upper()
    )

    cleaned_rates["rate_to_usd"] = pd.to_numeric(
        cleaned_rates["rate_to_usd"],
        errors="coerce",
    )

    cleaned_rates["_rate_date_key"] = pd.to_datetime(
        cleaned_rates["date"],
        errors="coerce",
    ).dt.normalize()

    # Invalid exchange-rate rows cannot be used for conversion.
    cleaned_rates = cleaned_rates.loc[
        cleaned_rates["currency"].notna()
        & cleaned_rates["_rate_date_key"].notna()
        & cleaned_rates["rate_to_usd"].notna()
        & (cleaned_rates["rate_to_usd"] > 0)
    ].copy()

    # More than one rate for the same currency and date could duplicate
    # order rows during the merge, so fail explicitly.
    duplicate_rates = cleaned_rates.duplicated(
        subset=["currency", "_rate_date_key"],
        keep=False,
    )

    if duplicate_rates.any():
        duplicated_values = cleaned_rates.loc[
            duplicate_rates,
            ["currency", "date"],
        ]

        raise ValueError(
            "Duplicate exchange rates found for the same currency and date:\n"
            f"{duplicated_values.to_string(index=False)}"
        )

    # Match each order to its daily rate using both currency and date.
    cleaned_orders = cleaned_orders.merge(
        cleaned_rates[
            [
                "currency",
                "_rate_date_key",
                "rate_to_usd",
            ]
        ],
        how="left",
        left_on=["currency", "_order_date_key"],
        right_on=["currency", "_rate_date_key"],
        validate="many_to_one",
    )

    # Default behavior: assume the original amount is already in USD.
    cleaned_orders["usd_amount"] = cleaned_orders["total_amount"]

    # Convert only non-USD orders that have a matching exchange rate.
    conversion_mask = (
        cleaned_orders["currency"].ne("USD")
        & cleaned_orders["rate_to_usd"].notna()
    )

    cleaned_orders.loc[conversion_mask, "usd_amount"] = (
        cleaned_orders.loc[conversion_mask, "total_amount"]
        * cleaned_orders.loc[conversion_mask, "rate_to_usd"]
    )

    cleaned_orders["usd_amount"] = cleaned_orders[
        "usd_amount"
    ].round(2)

    # Store valid dates in a consistent format. Missing dates remain missing.
    cleaned_orders["order_date"] = (
        cleaned_orders["_order_date_key"]
        .dt.strftime("%Y-%m-%d")
    )

    # Remove temporary join and calculation columns.
    cleaned_orders = cleaned_orders.drop(
        columns=[
            "_order_date_key",
            "_rate_date_key",
            "rate_to_usd",
        ]
    )

    return cleaned_orders.reset_index(drop=True)

@task(name="transform-customers")
def transform_customers(
    customers: pd.DataFrame,
) -> pd.DataFrame:
    """Prefect task that applies customer transformation rules."""
    logger = get_run_logger()

    try:
        source_row_count = len(customers)

        cleaned_customers = clean_customers(customers)

        logger.info(
            "Customer transformation completed: "
            "source_rows=%d, cleaned_rows=%d, duplicates_removed=%d",
            source_row_count,
            len(cleaned_customers),
            source_row_count - len(cleaned_customers),
        )

        missing_email_count = (
            cleaned_customers["email"]
            .eq("unknown@domain.com")
            .sum()
        )

        logger.info(
            "Customer cleaning result: missing_emails_replaced=%d",
            missing_email_count,
        )

        return cleaned_customers

    except (KeyError, TypeError, ValueError):
        logger.exception("Customer transformation failed")
        raise

@task(name="transform-orders")
def transform_orders(
    orders: pd.DataFrame,
    exchange_rates: pd.DataFrame,
) -> pd.DataFrame:
    """Prefect task that cleans orders and calculates USD amounts."""
    logger = get_run_logger()

    try:
        source_row_count = len(orders)

        cleaned_orders = clean_orders(
            orders,
            exchange_rates,
        )

        removed_count = source_row_count - len(cleaned_orders)

        logger.info(
            "Order transformation completed: "
            "source_rows=%d, cleaned_rows=%d, invalid_orders_removed=%d",
            source_row_count,
            len(cleaned_orders),
            removed_count,
        )

        missing_currency_count = (
            orders["currency"].isna()
            | orders["currency"].astype("string").str.strip().eq("")
        ).sum()

        logger.info(
            "Order cleaning result: missing_currencies_treated_as_usd=%d",
            missing_currency_count,
        )

        return cleaned_orders

    except (KeyError, TypeError, ValueError):
        logger.exception("Order transformation failed")
        raise