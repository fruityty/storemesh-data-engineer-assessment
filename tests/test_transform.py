import pandas as pd
import pytest

from etl.transform import (
    clean_customers,
    clean_orders,
    standardize_phone,
)


@pytest.mark.parametrize(
    ("input_phone", "expected_phone"),
    [
        ("+1 (555) 123-4567", "15551234567"),
        ("555-987-6543", "5559876543"),
        ("Ext 444", "444"),
        ("1-800-555-DINO", "1800555"),
        (None, None),
        ("no phone", None),
    ],
)
def test_standardize_phone(
    input_phone: object,
    expected_phone: str | None,
) -> None:
    """Phone cleaning should remove all non-numeric characters."""
    assert standardize_phone(input_phone) == expected_phone


def test_clean_customers_keeps_latest_record() -> None:
    """The latest signup_date should be retained for duplicate customers."""
    customers = pd.DataFrame(
        {
            "customer_id": [1, 1],
            "full_name": ["Alice Smith", "Alice Smith"],
            "email": [
                "alice@example.com",
                "alice.smith@example.com",
            ],
            "phone": [
                "+1 (555) 123-4567",
                "15551234567",
            ],
            "signup_date": [
                "2023-01-15",
                "2023-06-01",
            ],
        }
    )

    result = clean_customers(customers)

    assert len(result) == 1
    assert result.loc[0, "customer_id"] == 1
    assert result.loc[0, "email"] == "alice.smith@example.com"
    assert result.loc[0, "signup_date"] == "2023-06-01"


def test_clean_customers_replaces_missing_email() -> None:
    """Missing customer emails should use the required fallback value."""
    customers = pd.DataFrame(
        {
            "customer_id": [8],
            "full_name": ["Hannah Abbott"],
            "email": [None],
            "phone": [None],
            "signup_date": ["2023-07-01"],
        }
    )

    result = clean_customers(customers)

    assert result.loc[0, "email"] == "unknown@domain.com"


def test_clean_orders_filters_non_positive_amounts() -> None:
    """Zero and negative order amounts should be removed."""
    orders = pd.DataFrame(
        {
            "order_id": [101, 102, 103],
            "customer_id": [1, 2, 3],
            "order_date": [
                "2023-05-01",
                "2023-05-01",
                "2023-05-01",
            ],
            "total_amount": [100.0, 0.0, -50.0],
            "currency": ["USD", "USD", "USD"],
            "status": [
                "COMPLETED",
                "COMPLETED",
                "SYSTEM_ERROR",
            ],
        }
    )

    exchange_rates = pd.DataFrame(
        {
            "currency": [],
            "rate_to_usd": [],
            "date": [],
        }
    )

    result = clean_orders(orders, exchange_rates)

    assert result["order_id"].tolist() == [101]
    assert result.loc[0, "usd_amount"] == pytest.approx(100.0)


def test_clean_orders_converts_currency_using_daily_rate() -> None:
    """A non-USD order should use the matching currency and date rate."""
    orders = pd.DataFrame(
        {
            "order_id": [102],
            "customer_id": [2],
            "order_date": ["2023-05-01"],
            "total_amount": [200.0],
            "currency": ["EUR"],
            "status": ["COMPLETED"],
        }
    )

    exchange_rates = pd.DataFrame(
        {
            "currency": ["EUR"],
            "rate_to_usd": [1.10],
            "date": ["2023-05-01"],
        }
    )

    result = clean_orders(orders, exchange_rates)

    assert result.loc[0, "currency"] == "EUR"
    assert result.loc[0, "usd_amount"] == pytest.approx(220.0)


def test_clean_orders_uses_rate_from_matching_date() -> None:
    """Currency conversion should use the order-date rate."""
    orders = pd.DataFrame(
        {
            "order_id": [104],
            "customer_id": [1],
            "order_date": ["2023-05-02"],
            "total_amount": [300.0],
            "currency": ["EUR"],
            "status": ["COMPLETED"],
        }
    )

    exchange_rates = pd.DataFrame(
        {
            "currency": ["EUR", "EUR"],
            "rate_to_usd": [1.10, 1.12],
            "date": ["2023-05-01", "2023-05-02"],
        }
    )

    result = clean_orders(orders, exchange_rates)

    assert result.loc[0, "usd_amount"] == pytest.approx(336.0)


def test_clean_orders_treats_missing_currency_as_usd() -> None:
    """A blank currency should be normalized to USD."""
    orders = pd.DataFrame(
        {
            "order_id": [107],
            "customer_id": [5],
            "order_date": ["2023-05-05"],
            "total_amount": [120.0],
            "currency": [""],
            "status": ["COMPLETED"],
        }
    )

    exchange_rates = pd.DataFrame(
        {
            "currency": [],
            "rate_to_usd": [],
            "date": [],
        }
    )

    result = clean_orders(orders, exchange_rates)

    assert result.loc[0, "currency"] == "USD"
    assert result.loc[0, "usd_amount"] == pytest.approx(120.0)


def test_clean_orders_treats_missing_rate_as_usd() -> None:
    """A non-USD order without a matching rate should keep its amount."""
    orders = pd.DataFrame(
        {
            "order_id": [110],
            "customer_id": [8],
            "order_date": ["2023-05-07"],
            "total_amount": [89.0],
            "currency": ["EUR"],
            "status": ["COMPLETED"],
        }
    )

    exchange_rates = pd.DataFrame(
        {
            "currency": ["EUR"],
            "rate_to_usd": [1.10],
            "date": ["2023-05-01"],
        }
    )

    result = clean_orders(orders, exchange_rates)

    assert result.loc[0, "currency"] == "EUR"
    assert result.loc[0, "usd_amount"] == pytest.approx(89.0)