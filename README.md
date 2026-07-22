# Storemesh Data Engineer Technical Assessment

## Overview

This repository contains my solution for the Storemesh Data Engineer Technical Assessment.

## Project Structure

```text
storemesh-data-engineer-assessment/
├── data/
│   └── shopdata.db
├── scripts/
│   ├── inspect_db.py
│   └── run_exploration.py
├── sql_scripts/
│   └── exploration.sql
├── tests/
├── requirements.txt
├── README.md
└── .gitignore
```

## Requirements

* Python 3.12 or newer
* pandas
* Prefect 3.x
* pytest
* SQLite

Install the required Python packages with:

```bash
python -m pip install -r requirements.txt
```
The source database file is not included in this repository. To run the exploration locally, place the provided database at:

```text
data/shopdata.db
```

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
