{{ config(
    materialized='table',
    tags=['gold', 'report']
) }}

-- ============================================================================
-- GOLD LAYER - SALES BY CATEGORY REPORT
-- ============================================================================
-- Aggregated sales metrics by product category and department
--
-- Features:
-- - Revenue and order metrics per category
-- - Profit calculations
-- - Department-level rollups
-- - Category ranking
-- ============================================================================

WITH categories AS (
    SELECT
        category_id,
        category_name,
        department,
        category_key
    FROM {{ ref('dim_categories') }}
),

order_items AS (
    SELECT
        order_item_id,
        order_id,
        product_id,
        product_category,
        quantity,
        line_total,
        line_total_after_discount,
        estimated_profit,
        is_completed
    FROM {{ ref('fct_order_items') }}
    WHERE is_completed = TRUE
)

SELECT
    -- Category dimensions
    c.category_id,
    c.category_name,
    c.department,

    -- Order metrics
    COUNT(DISTINCT oi.order_id) AS total_orders,
    COUNT(oi.order_item_id) AS total_line_items,

    -- Unit metrics
    SUM(oi.quantity) AS total_units_sold,
    ROUND(AVG(oi.quantity), 2) AS avg_units_per_order,

    -- Revenue metrics
    ROUND(SUM(oi.line_total), 2) AS gross_revenue,
    ROUND(SUM(oi.line_total_after_discount), 2) AS net_revenue,
    ROUND(SUM(oi.line_total) - SUM(oi.line_total_after_discount), 2) AS total_discounts,

    -- Profit metrics
    ROUND(SUM(oi.estimated_profit), 2) AS total_profit,
    CASE
        WHEN SUM(oi.line_total_after_discount) > 0 THEN
            ROUND(SUM(oi.estimated_profit) / SUM(oi.line_total_after_discount) * 100, 2)
        ELSE 0
    END AS profit_margin_pct,

    -- Average order value
    ROUND(SUM(oi.line_total_after_discount) / NULLIF(COUNT(DISTINCT oi.order_id), 0), 2) AS avg_order_value,

    -- Category ranking by revenue
    RANK() OVER (ORDER BY SUM(oi.line_total_after_discount) DESC) AS revenue_rank,
    RANK() OVER (PARTITION BY c.department ORDER BY SUM(oi.line_total_after_discount) DESC) AS department_revenue_rank,

    -- Metadata
    CURRENT_TIMESTAMP() AS report_generated_at

FROM categories c
LEFT JOIN order_items oi ON c.category_key = oi.product_category
GROUP BY
    c.category_id,
    c.category_name,
    c.department
ORDER BY net_revenue DESC NULLS LAST
