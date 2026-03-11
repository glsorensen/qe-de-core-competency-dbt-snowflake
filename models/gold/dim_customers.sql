{{ config(
    materialized='table',
    tags=['gold', 'dimension']
) }}

-- ============================================================================
-- GOLD LAYER - CUSTOMER DIMENSION
-- ============================================================================
-- Business-ready customer dimension with aggregated metrics
--
-- Features:
-- - Complete customer profile
-- - Order history aggregations
-- - Customer segmentation based on purchase frequency
-- - Lifetime value metrics
-- - Activity status
-- ============================================================================

WITH customers AS (
    SELECT *
    FROM {{ ref('stg_customers') }}
),

orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
),

customer_orders AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(total_amount) AS total_revenue,
        MAX(order_date) AS most_recent_order_date
    FROM orders
    GROUP BY customer_id
)

SELECT
    -- Customer identifiers and core information
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.address,
    c.city,
    c.state,
    c.zip_code,
    c.created_at,

    -- Customer segmentation based on purchase frequency
    CASE
        WHEN co.total_orders IS NULL OR co.total_orders = 0 THEN 'never_purchased'
        WHEN co.total_orders = 1 THEN 'one_time'
        WHEN co.total_orders BETWEEN 2 AND 5 THEN 'occasional'
        WHEN co.total_orders BETWEEN 6 AND 10 THEN 'regular'
        ELSE 'champion'
    END AS customer_segment,

    -- Value tier based on lifetime revenue
    CASE
        WHEN co.total_revenue IS NULL OR co.total_revenue = 0 THEN 'no_value'
        WHEN co.total_revenue < 100 THEN 'low_value'
        WHEN co.total_revenue < 500 THEN 'medium_value'
        WHEN co.total_revenue < 1000 THEN 'high_value'
        ELSE 'vip'
    END AS value_tier,

    -- Activity status based on recency of orders
    CASE
        WHEN co.most_recent_order_date IS NULL THEN 'never_ordered'
        WHEN DATEDIFF('day', co.most_recent_order_date, CURRENT_DATE()) <= 30 THEN 'active'
        WHEN DATEDIFF('day', co.most_recent_order_date, CURRENT_DATE()) <= 90 THEN 'at_risk'
        ELSE 'churned'
    END AS activity_status

FROM customers c
LEFT JOIN customer_orders co ON c.customer_id = co.customer_id

