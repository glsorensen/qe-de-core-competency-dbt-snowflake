-- Silver Layer: Staging Orders Referential Integrity
-- Validates that all order customer_ids exist in customers table

SELECT
    DISTINCT o.customer_id
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL
