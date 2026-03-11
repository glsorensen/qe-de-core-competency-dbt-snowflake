-- Gold Layer: Dimension Customers Valid Segments
-- Validates that customer segments are valid values

SELECT
    customer_id,
    customer_segment
FROM {{ ref('dim_customers') }}
WHERE customer_segment NOT IN ('never_purchased', 'one_time', 'occasional', 'regular', 'champion')
