-- Gold Layer: Fact Orders Referential Integrity to Products
-- Validates that all order product_ids exist in product dimension

SELECT
    DISTINCT do.product_id
FROM {{ ref('dim_orders') }} do
LEFT JOIN {{ ref('dim_products') }} dp ON do.product_id = dp.product_id
WHERE dp.product_id IS NULL
