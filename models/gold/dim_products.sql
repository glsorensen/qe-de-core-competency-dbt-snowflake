{{ config(
    materialized='table',
    tags=['gold', 'dimension']
) }}

-- ============================================================================
-- GOLD LAYER - PRODUCT DIMENSION
-- ============================================================================
-- Business-ready product dimension with categorization
--
-- Features:
-- - Complete product catalog information
-- - Price categorization
-- - Product categorization for analysis
-- ============================================================================

WITH products AS (
    SELECT *
    FROM {{ ref('stg_products') }}
)

SELECT
    -- Product identifiers and details
    p.product_id,
    p.product_name,
    p.category,
    p.price,
    p.stock_quantity,
    p.description,

    -- Price categorization
    CASE
        WHEN p.price < 30 THEN 'budget'
        WHEN p.price < 60 THEN 'mid_range'
        ELSE 'premium'
    END AS price_category,

    -- Sales performance categorization (placeholder for future sales data)
    'no_sales' AS sales_performance,

    -- Product status
    CASE
        WHEN p.stock_quantity = 0 THEN 'no_orders'
        ELSE 'active'
    END AS product_status,

    -- Margin category (placeholder - actual margin data not in source)
    'medium_margin' AS margin_category

FROM products p

