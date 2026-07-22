-- Customer data quality checks
-- 1. vw_raw_customers: Contains customer demographics and contact info. 
-- 2. vw_raw_orders: Contains transactional data.
-- 3. vw_exchange_rates: Contains daily exchange rates to USD.

----------------------
-- vw_raw_customers --
----------------------

-- 1. Check duplicated customer_id in vw_raw_customers
SELECT *
FROM vw_raw_customers 
WHERE 
customer_id IN 
    ( SELECT customer_id 
      FROM vw_raw_customers 
      GROUP BY customer_id 
      HAVING COUNT(*) > 1 ) 
ORDER BY customer_id, signup_date;

-- 2. Check missing values rows in vw_raw_customers
SELECT *
FROM vw_raw_customers
WHERE customer_id IS NULL
   OR full_name IS NULL OR TRIM(full_name) = ''
   OR email IS NULL OR TRIM(email) = ''
   OR phone IS NULL OR TRIM(phone) = ''
   OR signup_date IS NULL OR TRIM(signup_date) = '';

-- 3. Check Phone non digits values in vw_raw_customers
SELECT
    customer_id,
    full_name,
    phone
FROM vw_raw_customers
WHERE phone GLOB '*[^0-9]*';
--  GLOB = string matches pattern

-------------------
-- vw_raw_orders --
-------------------
-- 4. Check missing values rows in vw_raw_orders
SELECT *
FROM vw_raw_orders
WHERE order_id IS NULL
   OR customer_id IS NULL
   OR order_date IS NULL OR TRIM(order_date) = ''
   OR total_amount IS NULL
   OR currency IS NULL OR TRIM(currency) = ''
   OR status IS NULL OR TRIM(status) = '';

-- 5. Check orders with invalid amounts in vw_raw_orders
SELECT *
FROM vw_raw_orders 
WHERE total_amount IS NULL OR total_amount <= 0;

-- 6. Check customer_id not match
SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.total_amount
FROM vw_raw_orders AS o
LEFT JOIN vw_raw_customers AS c
    ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;