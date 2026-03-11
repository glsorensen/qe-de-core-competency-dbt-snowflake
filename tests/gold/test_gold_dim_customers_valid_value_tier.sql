-- Gold Layer: Dimension Customers Valid Value Tier
-- Validates that value tier are valid values

SELECT
    customer_id,
    value_tier
FROM {{ ref('dim_customers') }}
WHERE value_tier NOT IN ('no_value', 'low_value', 'medium_value', 'high_value', 'vip')
