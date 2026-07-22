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