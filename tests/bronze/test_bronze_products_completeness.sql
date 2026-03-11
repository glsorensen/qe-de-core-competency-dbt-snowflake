-- Bronze Layer: Products Completeness Tests
-- Validates that raw product data has required fields populated

SELECT
    product_id,
    product_name,
    category,
    price,
    stock_quantity
FROM {{ source('raw', 'products') }}
WHERE
    product_id IS NULL
    OR product_name IS NULL
    OR category IS NULL
    OR price IS NULL
    OR stock_quantity IS NULL
