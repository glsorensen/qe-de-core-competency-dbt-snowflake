-- Gold Layer: Dimension Products Valid Price Category
-- Validates that price category are valid values

SELECT
    product_id,
    product_name,
    price_category
FROM {{ ref('dim_products') }}
WHERE price_category NOT IN ('budget', 'mid_range', 'premium')
