-- Bronze Layer: Products Price Validation
-- Validates that product prices are positive

SELECT
    product_id,
    product_name,
    price
FROM {{ source('raw', 'products') }}
WHERE price <= 0
