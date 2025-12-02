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
-- - Sales performance by product and category
-- - Profit margin analysis
-- - Product ranking and trends
-- - Discount impact analysis
-- ============================================================================

WITH order_items AS (
    SELECT *
    FROM {{ ref('fct_order_items') }}
    WHERE is_completed = TRUE  -- Only include completed orders
),

products AS (
    SELECT
        product_id,
        product_name,
        product_category,
        product_subcategory,
        brand,
        price AS current_price,
        cost AS current_cost,
        profit_margin_pct AS current_profit_margin_pct,
        price_category
    FROM {{ ref('stg_products') }}
)

SELECT
    -- Product dimensions
    p.product_id,
    p.product_name,
    p.product_category,
    p.product_subcategory,
    p.brand,
    p.price_category,

    -- Current pricing
    p.current_price,
    p.current_cost,
    p.current_profit_margin_pct,

    -- Sales volume metrics
    COUNT(DISTINCT oi.order_id) AS orders_containing_product,
    COUNT(DISTINCT oi.customer_id) AS unique_customers,
    SUM(oi.quantity) AS total_units_sold,

    -- Revenue metrics
    SUM(oi.line_total) AS gross_revenue,
    SUM(oi.discount_applied) AS total_discounts_given,
    SUM(oi.line_total_after_discount) AS net_revenue,

    -- Profit metrics (estimated using current cost)
    SUM(oi.estimated_cost) AS total_estimated_cost,
    SUM(oi.estimated_profit) AS total_estimated_profit,

    -- Average metrics
    ROUND(AVG(oi.unit_price), 2) AS avg_selling_price,
    ROUND(AVG(oi.line_total_after_discount), 2) AS avg_line_value,
    ROUND(AVG(oi.quantity), 2) AS avg_quantity_per_order,
    ROUND(AVG(oi.estimated_profit_margin_pct), 2) AS avg_profit_margin_pct,

    -- Discount analysis
    COUNT(DISTINCT CASE WHEN oi.has_discount THEN oi.order_item_id END) AS discounted_line_items,
    ROUND(AVG(oi.discount_percentage), 2) AS avg_discount_percentage,

    CASE
        WHEN COUNT(*) > 0 THEN
            ROUND((COUNT(CASE WHEN oi.has_discount THEN 1 END)::FLOAT / COUNT(*)) * 100, 2)
        ELSE 0
    END AS pct_sales_with_discount,

    -- Product performance flags
    CASE
        WHEN SUM(oi.line_total_after_discount) >= 10000 THEN 'top_performer'
        WHEN SUM(oi.line_total_after_discount) >= 5000 THEN 'strong_performer'
        WHEN SUM(oi.line_total_after_discount) >= 1000 THEN 'moderate_performer'
        ELSE 'low_performer'
    END AS performance_tier,

    CASE
        WHEN AVG(oi.estimated_profit_margin_pct) >= 40 THEN 'high_margin'
        WHEN AVG(oi.estimated_profit_margin_pct) >= 20 THEN 'medium_margin'
        ELSE 'low_margin'
    END AS margin_tier,

    -- Bulk purchase indicator
    CASE
        WHEN COUNT(CASE WHEN oi.is_bulk_purchase THEN 1 END) > 0 THEN TRUE
        ELSE FALSE
    END AS has_bulk_purchases,

    -- Metadata
    MAX(oi.dbt_updated_at) AS last_updated_at

FROM products p
LEFT JOIN order_items oi ON p.product_id = oi.product_id

GROUP BY
    p.product_id,
    p.product_name,
    p.product_category,
    p.product_subcategory,
    p.brand,
    p.price_category,
    p.current_price,
    p.current_cost,
    p.current_profit_margin_pct

-- Order by revenue to show best sellers first
ORDER BY net_revenue DESC
