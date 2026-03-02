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
-- - Sales performance metrics
-- - Discount and margin analysis
-- - Product categorization
-- ============================================================================

WITH products AS (
    SELECT *
    FROM {{ ref('stg_products') }}
),

order_items AS (
    SELECT *
    FROM {{ ref('stg_order_items') }}
),

product_sales AS (
    SELECT
        product_id,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(order_item_id) AS total_line_items,
        SUM(quantity) AS total_quantity_sold,
        SUM(line_total) AS total_revenue,
        SUM(line_total_after_discount) AS total_revenue_after_discount,
        SUM(discount_applied) AS total_discounts_given,
        AVG(discount_percentage) AS avg_discount_percentage,
        AVG(unit_price) AS avg_unit_price,
        MIN(unit_price) AS min_unit_price,
        MAX(unit_price) AS max_unit_price,
        SUM(CASE WHEN has_discount THEN 1 ELSE 0 END) AS items_sold_with_discount,
        MIN(created_at) AS first_sale_date,
        MAX(created_at) AS most_recent_sale_date
    FROM order_items
    GROUP BY product_id
)

SELECT
    -- Product identifiers
    p.product_id,
    p.product_name,
    p.sku,
    p.brand,
    
    -- Product categorization
    p.product_category,
    p.product_subcategory,
    p.price_category,
    
    -- Pricing information
    p.price AS current_price,
    p.cost AS current_cost,
    p.profit_margin_pct AS current_profit_margin_pct,
    p.profit_amount AS current_profit_amount,
    
    -- Sales metrics
    COALESCE(ps.total_orders, 0) AS total_orders,
    COALESCE(ps.total_line_items, 0) AS total_line_items,
    COALESCE(ps.total_quantity_sold, 0) AS total_quantity_sold,
    COALESCE(ps.total_revenue, 0) AS total_revenue,
    COALESCE(ps.total_revenue_after_discount, 0) AS total_revenue_after_discount,
    
    -- Calculate actual profit based on sales (revenue after discount - cost)
    COALESCE(
        ps.total_revenue_after_discount - (ps.total_quantity_sold * p.cost),
        0
    ) AS total_profit,
    
    -- Calculate realized profit margin
    CASE
        WHEN ps.total_revenue_after_discount > 0 THEN
            ROUND(
                ((ps.total_revenue_after_discount - (ps.total_quantity_sold * p.cost)) 
                / ps.total_revenue_after_discount) * 100,
                2
            )
        ELSE NULL
    END AS realized_profit_margin_pct,
    
    -- Discount metrics
    COALESCE(ps.total_discounts_given, 0) AS total_discounts_given,
    COALESCE(ps.avg_discount_percentage, 0) AS avg_discount_percentage,
    COALESCE(ps.items_sold_with_discount, 0) AS items_sold_with_discount,
    
    -- Discount penetration rate
    CASE
        WHEN ps.total_line_items > 0 THEN
            ROUND((ps.items_sold_with_discount::FLOAT / ps.total_line_items) * 100, 2)
        ELSE 0
    END AS discount_penetration_pct,
    
    -- Pricing analysis
    COALESCE(ps.avg_unit_price, p.price) AS avg_selling_price,
    COALESCE(ps.min_unit_price, p.price) AS min_selling_price,
    COALESCE(ps.max_unit_price, p.price) AS max_selling_price,
    
    -- Calculate price variance
    CASE
        WHEN ps.avg_unit_price IS NOT NULL AND p.price > 0 THEN
            ROUND(((ps.avg_unit_price - p.price) / p.price) * 100, 2)
        ELSE 0
    END AS price_variance_pct,
    
    -- Sales dates
    ps.first_sale_date,
    ps.most_recent_sale_date,
    
    -- Days since last sale
    CASE
        WHEN ps.most_recent_sale_date IS NOT NULL
        THEN DATEDIFF('day', ps.most_recent_sale_date, CURRENT_DATE())
        ELSE NULL
    END AS days_since_last_sale,
    
    -- Product age
    p.created_at AS product_created_at,
    p.days_since_created AS product_age_days,
    
    -- Product status
    p.is_active,
    
    -- Sales status classification
    CASE
        WHEN ps.product_id IS NULL THEN 'never_sold'
        WHEN ps.most_recent_sale_date IS NULL THEN 'never_sold'
        WHEN DATEDIFF('day', ps.most_recent_sale_date, CURRENT_DATE()) <= 30 THEN 'active'
        WHEN DATEDIFF('day', ps.most_recent_sale_date, CURRENT_DATE()) <= 90 THEN 'slow_moving'
        ELSE 'stale'
    END AS sales_status,
    
    -- Performance tier based on revenue
    CASE
        WHEN COALESCE(ps.total_revenue, 0) >= 5000 THEN 'top_performer'
        WHEN COALESCE(ps.total_revenue, 0) >= 2000 THEN 'strong_performer'
        WHEN COALESCE(ps.total_revenue, 0) >= 500 THEN 'moderate_performer'
        WHEN COALESCE(ps.total_revenue, 0) > 0 THEN 'low_performer'
        ELSE 'no_sales'
    END AS performance_tier,
    
    -- Margin tier based on realized profit margin
    CASE
        WHEN ps.total_revenue_after_discount > 0 AND
             ((ps.total_revenue_after_discount - (ps.total_quantity_sold * p.cost)) 
              / ps.total_revenue_after_discount) >= 0.40 THEN 'high_margin'
        WHEN ps.total_revenue_after_discount > 0 AND
             ((ps.total_revenue_after_discount - (ps.total_quantity_sold * p.cost)) 
              / ps.total_revenue_after_discount) >= 0.20 THEN 'medium_margin'
        WHEN ps.total_revenue_after_discount > 0 THEN 'low_margin'
        ELSE 'no_sales'
    END AS margin_tier,
    
    -- Metadata
    p.dbt_updated_at

FROM products p
LEFT JOIN product_sales ps ON p.product_id = ps.product_id
