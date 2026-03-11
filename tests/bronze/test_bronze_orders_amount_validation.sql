-- Bronze Layer: Orders Amount Validation
-- Validates that order amounts are positive

SELECT
    order_id,
    total_amount
FROM {{ source('raw', 'orders') }}
WHERE total_amount < 0
