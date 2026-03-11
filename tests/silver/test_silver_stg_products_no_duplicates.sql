-- Silver Layer: Staging Products Duplicate Detection
-- Validates that there are no duplicate products in staging

SELECT
    product_id,
    COUNT(*) as occurrences
FROM {{ ref('stg_products') }}
GROUP BY product_id
HAVING COUNT(*) > 1
