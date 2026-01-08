{{ config(
    materialized='table',
    tags=['gold', 'report']
) }}

-- ============================================================================
-- GOLD LAYER - SALES BY CATEGORY REPORT
-- ============================================================================
-- Executive-ready sales performance report by product category
--
-- Features:
-- - Category-level sales aggregation
-- - Department rollup
-- - Product diversity metrics
-- - Order and revenue metrics
-- ============================================================================

WITH categories AS (
    SELECT *
    FROM {{ ref('dim_categories') }}
),

products AS (
    SELECT *
    FROM {{ ref('stg_products') }}
),

order_items AS (
    SELECT *
    FROM {{ ref('stg_order_items') }}
),

-- Join products to their categories
products_with_categories AS (
    SELECT
        p.*,
        c.category_name,
        c.department
    FROM products p
    INNER JOIN categories c ON p.category_id = c.category_id
),

-- Calculate sales metrics per category
category_sales AS (
    SELECT
        pwc.category_id,
        pwc.category_name,
        pwc.department,
        
        -- Product metrics
        COUNT(DISTINCT pwc.product_id) AS total_products,
        COUNT(DISTINCT pwc.brand) AS total_brands,
        
        -- Sales metrics
        COUNT(DISTINCT oi.order_id) AS total_orders,
        COUNT(oi.order_item_id) AS total_line_items,
        SUM(oi.quantity) AS total_quantity_sold,
        SUM(oi.line_total) AS total_revenue,
        SUM(oi.line_total_after_discount) AS total_revenue_after_discount,
        SUM(oi.discount_applied) AS total_discounts_given,
        
        -- Average metrics
        AVG(oi.unit_price) AS avg_unit_price,
        AVG(oi.line_total) AS avg_line_total,
        AVG(oi.discount_percentage) AS avg_discount_percentage,
        
        -- Min/Max metrics
        MIN(oi.unit_price) AS min_unit_price,
        MAX(oi.unit_price) AS max_unit_price
        
    FROM products_with_categories pwc
    LEFT JOIN order_items oi ON pwc.product_id = oi.product_id
    GROUP BY
        pwc.category_id,
        pwc.category_name,
        pwc.department
)

SELECT
    -- Category identifiers
    category_id,
    category_name,
    department,
    
    -- Product diversity
    total_products,
    total_brands,
    
    -- Sales volume
    COALESCE(total_orders, 0) AS total_orders,
    COALESCE(total_line_items, 0) AS total_line_items,
    COALESCE(total_quantity_sold, 0) AS total_quantity_sold,
    
    -- Revenue metrics
    COALESCE(total_revenue, 0) AS total_revenue,
    COALESCE(total_revenue_after_discount, 0) AS total_revenue_after_discount,
    COALESCE(total_discounts_given, 0) AS total_discounts_given,
    
    -- Average metrics
    COALESCE(avg_unit_price, 0) AS avg_unit_price,
    COALESCE(avg_line_total, 0) AS avg_line_total,
    COALESCE(avg_discount_percentage, 0) AS avg_discount_percentage,
    
    -- Price range
    COALESCE(min_unit_price, 0) AS min_unit_price,
    COALESCE(max_unit_price, 0) AS max_unit_price,
    
    -- Calculated metrics
    CASE
        WHEN total_products > 0 THEN
            ROUND(total_revenue::FLOAT / total_products, 2)
        ELSE 0
    END AS revenue_per_product,
    
    CASE
        WHEN total_orders > 0 THEN
            ROUND(total_revenue::FLOAT / total_orders, 2)
        ELSE 0
    END AS avg_order_value,
    
    -- Category performance tier based on revenue
    CASE
        WHEN COALESCE(total_revenue, 0) >= 2000 THEN 'top_performer'
        WHEN COALESCE(total_revenue, 0) >= 1000 THEN 'strong_performer'
        WHEN COALESCE(total_revenue, 0) >= 500 THEN 'moderate_performer'
        WHEN COALESCE(total_revenue, 0) > 0 THEN 'low_performer'
        ELSE 'no_sales'
    END AS performance_tier,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM category_sales
ORDER BY total_revenue DESC
