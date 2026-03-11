-- Gold Layer: Dimension Customers Duplicate Detection
-- Validates that there are no duplicate customers in dimension

SELECT
    customer_id,
    COUNT(*) as occurrences
FROM {{ ref('dim_customers') }}
GROUP BY customer_id
HAVING COUNT(*) > 1
