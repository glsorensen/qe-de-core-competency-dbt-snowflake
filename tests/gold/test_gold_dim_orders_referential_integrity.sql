-- Gold Layer: Fact Orders Referential Integrity to Customers
-- Validates that all order customer_ids exist in customer dimension

SELECT
    DISTINCT do.customer_id
FROM {{ ref('dim_orders') }} do
LEFT JOIN {{ ref('dim_customers') }} dc ON do.customer_id = dc.customer_id
WHERE dc.customer_id IS NULL
