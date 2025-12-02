{{ config(
    materialized='table',
    tags=['gold', 'report']
) }}

-- ============================================================================
-- GOLD LAYER - CUSTOMER METRICS REPORT
-- ============================================================================
-- Executive-ready customer analytics report
--
-- Features:
-- - Customer segmentation analysis
-- - Value tier distribution
-- - Activity status tracking
-- - Cohort metrics by acquisition channel
-- ============================================================================

WITH customers AS (
    SELECT *
    FROM {{ ref('dim_customers') }}
)

SELECT
    -- Grouping dimensions
    customer_segment,
    value_tier,
    activity_status,
    acquisition_channel,

    -- Customer counts
    COUNT(DISTINCT customer_id) AS customer_count,

    -- Order metrics
    SUM(total_orders) AS total_orders,
    SUM(completed_orders) AS completed_orders,
    SUM(cancelled_orders) AS cancelled_orders,

    -- Calculate completion and cancellation rates
    CASE
        WHEN SUM(total_orders) > 0 THEN
            ROUND((SUM(completed_orders)::FLOAT / SUM(total_orders)) * 100, 2)
        ELSE 0
    END AS order_completion_rate_pct,

    CASE
        WHEN SUM(total_orders) > 0 THEN
            ROUND((SUM(cancelled_orders)::FLOAT / SUM(total_orders)) * 100, 2)
        ELSE 0
    END AS order_cancellation_rate_pct,

    -- Revenue metrics
    SUM(lifetime_revenue) AS total_revenue,
    ROUND(AVG(lifetime_revenue), 2) AS avg_customer_lifetime_value,
    ROUND(AVG(avg_order_value), 2) AS avg_order_value,

    -- Revenue percentiles
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY lifetime_revenue), 2) AS revenue_p25,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY lifetime_revenue), 2) AS revenue_median,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY lifetime_revenue), 2) AS revenue_p75,
    ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY lifetime_revenue), 2) AS revenue_p90,

    -- Customer behavior metrics
    ROUND(AVG(customer_tenure_days), 0) AS avg_tenure_days,
    ROUND(AVG(days_since_last_order), 0) AS avg_days_since_last_order,
    ROUND(AVG(customer_age), 1) AS avg_customer_age,

    -- Discount usage
    SUM(orders_with_discount) AS total_orders_with_discount,
    SUM(total_discounts_received) AS total_discount_amount,
    ROUND(AVG(total_discounts_received), 2) AS avg_discount_per_customer,

    CASE
        WHEN SUM(total_orders) > 0 THEN
            ROUND((SUM(orders_with_discount)::FLOAT / SUM(total_orders)) * 100, 2)
        ELSE 0
    END AS discount_usage_rate_pct,

    -- Customer flags (percentage of customers)
    ROUND((SUM(CASE WHEN has_purchased THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100, 2) AS pct_customers_purchased,
    ROUND((SUM(CASE WHEN has_cancelled_order THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100, 2) AS pct_customers_with_cancellation,
    ROUND((SUM(CASE WHEN uses_discounts THEN 1 ELSE 0 END)::FLOAT / COUNT(*)) * 100, 2) AS pct_customers_using_discounts,

    -- Metadata
    MAX(dbt_updated_at) AS last_updated_at

FROM customers
GROUP BY
    customer_segment,
    value_tier,
    activity_status,
    acquisition_channel

HAVING customer_count > 0  -- Only include groups with at least one customer

ORDER BY
    total_revenue DESC
