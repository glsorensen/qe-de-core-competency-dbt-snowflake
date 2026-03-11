-- Gold Layer: Dimension Products Duplicate Detection
-- Validates that there are no duplicate products in dimension

SELECT
    product_id,
    COUNT(*) as occurrences
FROM {{ ref('dim_products') }}
GROUP BY product_id
HAVING COUNT(*) > 1
