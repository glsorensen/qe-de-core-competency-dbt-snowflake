-- Silver Layer: Staging Orders Duplicate Detection
-- Validates that there are no duplicate orders in staging

SELECT
    order_id,
    COUNT(*) as occurrences
FROM {{ ref('stg_orders') }}
GROUP BY order_id
HAVING COUNT(*) > 1
