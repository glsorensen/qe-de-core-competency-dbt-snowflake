-- Bronze Layer: Orders Completeness Tests
-- Validates that raw order data has required fields populated

SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    order_date,
    total_amount,
    status
FROM {{ source('raw', 'orders') }}
WHERE
    order_id IS NULL
    OR customer_id IS NULL
    OR product_id IS NULL
    OR quantity IS NULL
    OR order_date IS NULL
    OR total_amount IS NULL
    OR status IS NULL
