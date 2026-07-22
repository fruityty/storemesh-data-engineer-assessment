# Storemesh Data Engineer Technical Assessment

## Overview

This repository contains my solution for the Storemesh Data Engineer Technical Assessment.

The solution includes:

* SQL queries for exploring the source data
* A modular ETL pipeline orchestrated with Prefect
* Independent unit tests for transformation logic
* A customer lifetime value analytical query
* Setup and execution instructions

## Deliverables

| Requirement                      | Location                      |
| -------------------------------- | ----------------------------- |
| Prefect pipeline                 | `pipeline.py`                 |
| Modular ETL logic                | `etl/`                        |
| Unit tests                       | `tests/test_transform.py`     |
| Data exploration SQL             | `sql_scripts/exploration.sql` |
| Customer lifetime value SQL      | `sql_scripts/clv_report.sql`  |
| Python dependencies              | `requirements.txt`            |
| Setup and execution instructions | `README.md`                   |

## Project Structure

```text
storemesh-data-engineer-assessment/
├── data/
│   └── shopdata.db             # Provided separately; not committed
├── etl/
│   ├── __init__.py
│   ├── config.py
│   ├── extract.py
│   ├── transform.py
│   └── load.py
├── scripts/
│   ├── inspect_db.py
│   ├── inspect_analytic.py
│   ├── run_clv_report.py
│   └── run_exploration.py
├── sql_scripts/
│   ├── exploration.sql
│   └── clv_report.sql
├── tests/
│   └── test_transform.py
├── pipeline.py
├── requirements.txt
├── README.md
└── .gitignore
```

`analytics.db` is generated after the ETL pipeline runs and is not required to be included in the repository.

## Requirements

* Python 3.12 or newer
* pandas
* Prefect 3.x
* pytest
* SQLite support through Python's built-in `sqlite3` module

## Setup

Create and activate a Python virtual environment.

### Windows

```bash
python -m venv .venv
.venv\Scripts\activate
```

### macOS or Linux

```bash
python -m venv .venv
source .venv/bin/activate
```

Install the required Python packages:

```bash
python -m pip install -r requirements.txt
```

The source database file is not included in this repository. Place the provided database at:

```text
data/shopdata.db
```

The pipeline reads the source database through the following read-only views:

* `vw_raw_customers`
* `vw_raw_orders`
* `vw_exchange_rates`

## Part 1: Data Exploration and Understanding

## Data Quality Findings

The queries in `sql_scripts/exploration.sql` identified the following issues:

1. **Duplicate customer records**

   `vw_raw_customers` Some `customer_id` values appear more than once with different information.

   Customer IDs `1, 2` appears twice with different email addresses and signup dates.

2. **Missing customer information**

   `vw_raw_customers` Some customer records contain missing information.

   Customer IDs `2, 5, 8` missing email or phone values.

3. **Inconsistent customer phone formats**

   `vw_raw_customers` Several phone numbers contain non-numeric characters, such as

   `+1 (555) 123-4567`, `555-987-6543`, `1-800-555-DINO`, and `Ext 444`.

4. **Missing order fields**

   `vw_raw_orders` Orders `107` and `116` have missing currency values.

   Order `117` has a missing `order_date`.

5. **Invalid order amounts**

   `vw_raw_orders` Three orders have a `total_amount` less than or equal to zero:

   | order_id | total_amount |
   | -------: | -----------: |
   |      103 |       -50.00 |
   |      113 |      -100.00 |
   |      114 |         0.00 |

6. **Unknown customer references**

   `vw_raw_orders` Orders `106` and `118` reference customer ID `99`,

   which does not exist   in the customer source view.

## Running the Exploration Queries

Run the exploration script from the project root:

```bash
python scripts/run_exploration.py
```

The SQL queries are stored in:

```text
sql_scripts/exploration.sql
```

The exploration queries check for:

* duplicate customer records
* missing customer information
* inconsistent phone formats
* missing order fields
* zero or negative order amounts
* unknown customer references
* duplicate order identifiers

## Part 2: ETL Pipeline

The ETL pipeline is defined in:

```text
pipeline.py
```

The implementation is separated into modular components:

| Module             | Responsibility                     |
| ------------------ | ---------------------------------- |
| `etl/config.py`    | Project and database paths         |
| `etl/extract.py`   | Read source views from SQLite      |
| `etl/transform.py` | Customer and order cleaning rules  |
| `etl/load.py`      | Load analytical tables into SQLite |
| `pipeline.py`      | Prefect flow orchestration         |

### Pipeline Flow

The Prefect pipeline performs the following steps:

1. Extracts customers, orders, and exchange rates from the source views.
2. Deduplicates customers by retaining the record with the latest `signup_date`.
3. Standardizes phone numbers by removing non-numeric characters.
4. Replaces missing or blank customer emails with `unknown@domain.com`.
5. Removes orders whose `total_amount` is zero, negative, or invalid.
6. Normalizes missing or blank currencies to USD.
7. Converts non-USD orders using the exchange rate matching both currency and order date.
8. Treats orders without a matching exchange rate as already denominated in USD.
9. Loads the cleaned data into `analytics.db`.

### Prefect Usage

The pipeline uses:

* a Prefect `@flow` for pipeline orchestration
* Prefect `@task` functions for extraction, customer transformation, order transformation, and loading
* task-level retries for extraction and loading
* Prefect logging for row counts, cleaning results, task progress, and failures
* exception handling that logs errors before allowing the flow to fail

### Run the Pipeline

From the project root, run:

```bash
python pipeline.py
```

Prefect starts a temporary local server for the flow run.

A successful run creates:

```text
analytics.db
```

The analytical database contains:

* `dim_customers`
* `fct_orders`

Expected row counts for the provided source data:

| Table           | Expected rows |
| --------------- | ------------: |
| `dim_customers` |            10 |
| `fct_orders`    |            17 |

The pipeline can be rerun safely. The analytical tables are replaced with the latest transformed results.

## Part 3: Unit Tests

The unit tests are stored in:

```text
tests/test_transform.py
```

The tests call pure transformation functions directly using dummy values and pandas DataFrames. They do not depend on:

* the source SQLite database
* `analytics.db`
* a live Prefect server
* external services

This isolates the business logic from extraction, orchestration, and loading.

### Test Coverage

The tests cover:

* removal of non-numeric phone characters
* missing phone handling
* phone strings containing no digits
* customer deduplication using the latest signup date
* missing-email replacement
* filtering of zero and negative order amounts
* currency conversion using the correct daily exchange rate
* exchange-rate matching by both date and currency
* missing currency fallback to USD
* missing exchange-rate fallback to the original amount

### Run the Tests

Run all tests from the project root:

```bash
python -m pytest -v
```

To run only the transformation tests:

```bash
python -m pytest tests/test_transform.py -v
```

Current test result:

```text
13 passed
```

## Part 4: Customer Lifetime Value Report

The customer lifetime value query is stored at:

```text
sql_scripts/clv_report.sql
```

The query:

1. Aggregates order count and USD revenue by customer.
2. Joins the aggregated results to `dim_customers`.
3. Retains customers without orders by using a `LEFT JOIN`.
4. Uses `COALESCE` to return zero orders and zero lifetime value where appropriate.
5. Derives the customer cohort from the signup year and month.
6. Ranks customers by lifetime value in descending order.

The report contains:

* `customer_id`
* `full_name`
* `total_orders_placed`
* `lifetime_value_usd`
* `customer_cohort`

### Run the CLV Report

Run:

```bash
python scripts/run_clv_report.py
```

The query reads from the generated `analytics.db`, so the ETL pipeline must be run first.

## End-to-End Execution

After setting up the environment and adding `data/shopdata.db`, run the complete solution in this order:

```bash
python scripts/run_exploration.py
python pipeline.py
python -m pytest -v
python scripts/run_clv_report.py
```

A successful execution should produce 10 rows in `dim_customers`, 17 rows in `fct_orders`, and 13 passing unit tests.

## Transformation Assumptions

* Missing or blank currencies are treated as USD.
* Non-USD orders without a matching exchange rate retain their original amount as `usd_amount`.
* Exchange rates are matched using both currency and order date.
* Missing order dates remain null because no reliable replacement rule is provided.
* A missing order date does not cause an order to be removed when the order is already denominated in USD.
* Phone numbers are standardized by removing non-numeric characters.
* Country-specific phone-number validity is not inferred because no country information is provided.
* Missing or blank phone numbers remain null.
* `CANCELLED` and `PENDING` orders are retained because the required cleaning rule only removes orders with `total_amount <= 0`.
* Orders referencing an unknown customer remain in `fct_orders`.
* Unknown-customer orders do not appear in the CLV report because there is no corresponding record in `dim_customers`.
* Customer lifetime value includes all retained orders regardless of status because no status-based exclusion rule was specified.
