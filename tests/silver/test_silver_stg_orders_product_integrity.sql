-- Silver Layer: Staging Orders Product Referential Integrity
-- Validates that all order product_ids exist in products table

SELECT
    DISTINCT o.product_id
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_products') }} p ON o.product_id = p.product_id
WHERE p.product_id IS NULL
