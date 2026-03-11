-- Silver Layer: Staging Customers Duplicate Detection
-- Validates that there are no duplicate customers in staging

SELECT
    customer_id,
    COUNT(*) as occurrences
FROM {{ ref('stg_customers') }}
GROUP BY customer_id
HAVING COUNT(*) > 1
