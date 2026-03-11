{{ config(
    materialized='table',
    tags=['gold', 'report']
) }}

-- ============================================================================
-- GOLD LAYER - PRODUCT PERFORMANCE REPORT
-- ============================================================================
-- Executive-ready product analytics report
--
-- Features:
-- - Sales performance by product
-- - Revenue and order metrics
-- - Product ranking and categorization
-- ============================================================================

WITH dim_products AS (
    SELECT *
    FROM {{ ref('dim_products') }}
),

dim_orders AS (
    SELECT
        product_id,
        quantity,
        total_amount,
        status
    FROM {{ ref('dim_orders') }}
),

product_sales AS (
    SELECT
        product_id,
        COUNT(DISTINCT CASE WHEN status = 'completed' THEN product_id END) AS completed_orders,
        SUM(quantity) AS total_units_sold,
        SUM(total_amount) AS total_revenue,
        AVG(total_amount) AS avg_order_value
    FROM dim_orders
    GROUP BY product_id
)

SELECT
    -- Product dimensions
    p.product_id,
    p.product_name,
    p.category,
    p.price,
    p.price_category,

    -- Sales metrics
    COALESCE(ps.completed_orders, 0) AS orders_containing_product,
    COALESCE(ps.total_units_sold, 0) AS total_units_sold,
    COALESCE(ps.total_revenue, 0) AS total_revenue,
    COALESCE(ps.avg_order_value, 0) AS avg_order_value,

    -- Product performance tier
    CASE
        WHEN COALESCE(ps.total_revenue, 0) >= 1000 THEN 'top_performer'
        WHEN COALESCE(ps.total_revenue, 0) >= 500 THEN 'strong_performer'
        WHEN COALESCE(ps.total_revenue, 0) >= 100 THEN 'moderate_performer'
        ELSE 'low_performer'
    END AS performance_tier,

    -- Margin category (based on price tier as proxy)
    p.margin_category,

    -- Stock status
    p.product_status,

    -- Stock information
    p.stock_quantity

FROM dim_products p
LEFT JOIN product_sales ps ON p.product_id = ps.product_id

ORDER BY ps.total_revenue DESC NULLS LAST

