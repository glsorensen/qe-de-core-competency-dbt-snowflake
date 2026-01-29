{{ config(
    materialized='table',
    tags=['gold', 'dimension']
) }}

-- ============================================================================
-- GOLD LAYER - PRODUCT DIMENSION
-- ============================================================================
-- Business-ready product dimension with aggregated sales metrics
--
-- Features:
-- - Complete product catalog information
-- - Sales performance aggregations
-- - Profitability metrics
-- - Product ranking and categorization
-- ============================================================================

WITH products AS (
    SELECT *
    FROM {{ ref('stg_products') }}
),

order_items AS (
    SELECT *
    FROM {{ ref('stg_order_items') }}
),

orders AS (
    SELECT
        order_id,
        order_date,
        is_completed,
        is_cancelled
    FROM {{ ref('stg_orders') }}
),

product_sales AS (
    SELECT
        oi.product_id,
        COUNT(DISTINCT oi.order_id) AS total_orders,
        COUNT(DISTINCT CASE WHEN o.is_completed THEN oi.order_id END) AS completed_orders,
        COUNT(DISTINCT CASE WHEN o.is_cancelled THEN oi.order_id END) AS cancelled_orders,
        SUM(oi.quantity) AS total_units_sold,
        SUM(oi.line_total) AS total_revenue,
        AVG(oi.line_total) AS avg_line_revenue,
        MIN(o.order_date) AS first_order_date,
        MAX(o.order_date) AS most_recent_order_date,
        COUNT(DISTINCT o.order_date) AS days_with_sales
    FROM order_items oi
    INNER JOIN orders o ON oi.order_id = o.order_id
    WHERE o.is_completed = TRUE
    GROUP BY oi.product_id
)

SELECT
    -- Product identifiers
    p.product_id,
    p.sku,
    
    -- Product details
    p.product_name,
    p.brand,
    p.product_category,
    p.product_subcategory,
    
    -- Pricing information
    p.price,
    p.cost,
    p.profit_margin_pct,
    p.profit_amount AS unit_profit,
    p.price_category,
    
    -- Product lifecycle
    p.created_at AS product_created_at,
    p.updated_at AS product_updated_at,
    p.days_since_created AS product_age_days,
    p.is_active,
    
    -- Sales metrics
    COALESCE(ps.total_orders, 0) AS total_orders,
    COALESCE(ps.completed_orders, 0) AS completed_orders,
    COALESCE(ps.cancelled_orders, 0) AS cancelled_orders,
    COALESCE(ps.total_units_sold, 0) AS total_units_sold,
    COALESCE(ps.total_revenue, 0) AS total_revenue,
    COALESCE(ps.avg_line_revenue, 0) AS avg_line_revenue,
    ps.first_order_date,
    ps.most_recent_order_date,
    
    -- Days since last sale
    CASE
        WHEN ps.most_recent_order_date IS NOT NULL
        THEN DATEDIFF('day', ps.most_recent_order_date, CURRENT_DATE())
        ELSE NULL
    END AS days_since_last_sale,
    
    COALESCE(ps.days_with_sales, 0) AS days_with_sales,
    
    -- Performance categorization
    CASE
        WHEN COALESCE(ps.total_units_sold, 0) = 0 THEN 'no_sales'
        WHEN ps.total_units_sold < 10 THEN 'low_seller'
        WHEN ps.total_units_sold < 50 THEN 'medium_seller'
        ELSE 'high_seller'
    END AS sales_performance,
    
    -- Product health status
    CASE
        WHEN NOT p.is_active THEN 'inactive'
        WHEN COALESCE(ps.total_orders, 0) = 0 THEN 'no_orders'
        WHEN DATEDIFF('day', ps.most_recent_order_date, CURRENT_DATE()) > 90 THEN 'stale'
        WHEN DATEDIFF('day', ps.most_recent_order_date, CURRENT_DATE()) > 30 THEN 'slow_moving'
        ELSE 'active'
    END AS product_status,
    
    -- Profitability assessment
    CASE
        WHEN p.profit_margin_pct >= 40 THEN 'high_margin'
        WHEN p.profit_margin_pct >= 20 THEN 'medium_margin'
        ELSE 'low_margin'
    END AS margin_category,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM products p
LEFT JOIN product_sales ps ON p.product_id = ps.product_id
