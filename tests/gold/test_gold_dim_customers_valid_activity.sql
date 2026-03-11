-- Gold Layer: Dimension Customers Valid Activity Status
-- Validates that activity status are valid values

SELECT
    customer_id,
    activity_status
FROM {{ ref('dim_customers') }}
WHERE activity_status NOT IN ('never_ordered', 'active', 'at_risk', 'churned')
