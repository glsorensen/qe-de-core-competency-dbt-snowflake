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
-- - Customer segmentation
-- - Lifetime value metrics
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
        COUNT(DISTINCT CASE WHEN is_completed THEN order_id END) AS completed_orders,
        COUNT(DISTINCT CASE WHEN is_cancelled THEN order_id END) AS cancelled_orders,
        SUM(total_amount) AS total_revenue,
        AVG(total_amount) AS avg_order_value,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS most_recent_order_date,
        SUM(CASE WHEN has_discount THEN 1 ELSE 0 END) AS orders_with_discount,
        SUM(discount_amount) AS total_discounts_received
    FROM orders
    GROUP BY customer_id
)

SELECT
    -- Customer identifiers
    c.customer_id,
    c.email,

    -- Personal information
    c.first_name,
    c.last_name,
    c.first_name || ' ' || c.last_name AS full_name,
    c.phone_clean AS phone,
    c.date_of_birth,
    c.customer_age,

    -- Acquisition information
    c.acquisition_channel,
    c.created_at AS account_created_at,
    c.days_since_signup AS customer_tenure_days,

    -- Order metrics
    COALESCE(co.total_orders, 0) AS total_orders,
    COALESCE(co.completed_orders, 0) AS completed_orders,
    COALESCE(co.cancelled_orders, 0) AS cancelled_orders,
    COALESCE(co.total_revenue, 0) AS lifetime_revenue,
    COALESCE(co.avg_order_value, 0) AS avg_order_value,
    co.first_order_date,
    co.most_recent_order_date,

    -- Calculate days since last order
    CASE
        WHEN co.most_recent_order_date IS NOT NULL
        THEN DATEDIFF('day', co.most_recent_order_date, CURRENT_DATE())
        ELSE NULL
    END AS days_since_last_order,

    -- Discount usage
    COALESCE(co.orders_with_discount, 0) AS orders_with_discount,
    COALESCE(co.total_discounts_received, 0) AS total_discounts_received,

    -- Customer segmentation
    CASE
        WHEN co.total_orders IS NULL OR co.total_orders = 0 THEN 'never_purchased'
        WHEN co.total_orders = 1 THEN 'one_time'
        WHEN co.total_orders BETWEEN 2 AND 5 THEN 'occasional'
        WHEN co.total_orders BETWEEN 6 AND 10 THEN 'regular'
        ELSE 'champion'
    END AS customer_segment,

    -- Value tier
    CASE
        WHEN co.total_revenue IS NULL OR co.total_revenue = 0 THEN 'no_value'
        WHEN co.total_revenue < 100 THEN 'low_value'
        WHEN co.total_revenue < 500 THEN 'medium_value'
        WHEN co.total_revenue < 1000 THEN 'high_value'
        ELSE 'vip'
    END AS value_tier,

    -- Activity status
    CASE
        WHEN co.most_recent_order_date IS NULL THEN 'never_ordered'
        WHEN DATEDIFF('day', co.most_recent_order_date, CURRENT_DATE()) <= 30 THEN 'active'
        WHEN DATEDIFF('day', co.most_recent_order_date, CURRENT_DATE()) <= 90 THEN 'at_risk'
        ELSE 'churned'
    END AS activity_status,

    -- Flags
    CASE WHEN co.total_orders >= 1 THEN TRUE ELSE FALSE END AS has_purchased,
    CASE WHEN co.cancelled_orders > 0 THEN TRUE ELSE FALSE END AS has_cancelled_order,
    CASE WHEN co.orders_with_discount > 0 THEN TRUE ELSE FALSE END AS uses_discounts,

    -- Metadata
    c.dbt_updated_at

FROM customers c
LEFT JOIN customer_orders co ON c.customer_id = co.customer_id
