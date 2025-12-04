{{ config(
    materialized='table',
    tags=['gold', 'dimension']
) }}

-- ============================================================================
-- GOLD LAYER - CATEGORY DIMENSION
-- ============================================================================
-- Business-ready dimension table for category analysis
--
-- Features:
-- - Clean category attributes
-- - Product count per category
-- - Department grouping
-- ============================================================================

WITH categories AS (
    SELECT *
    FROM {{ ref('stg_categories') }}
),

products AS (
    SELECT
        product_category,
        COUNT(*) AS product_count,
        AVG(price) AS avg_product_price,
        MIN(price) AS min_product_price,
        MAX(price) AS max_product_price
    FROM {{ ref('stg_products') }}
    GROUP BY product_category
)

SELECT
    -- Primary key
    c.category_id,

    -- Category attributes
    c.category_name,
    c.department,
    c.category_key,

    -- Product statistics
    COALESCE(p.product_count, 0) AS product_count,
    ROUND(p.avg_product_price, 2) AS avg_product_price,
    p.min_product_price,
    p.max_product_price,

    -- Category flags
    CASE
        WHEN COALESCE(p.product_count, 0) = 0 THEN 'empty'
        WHEN p.product_count < 3 THEN 'small'
        WHEN p.product_count < 10 THEN 'medium'
        ELSE 'large'
    END AS category_size,

    -- Date fields
    c.created_at,
    c.days_since_created,

    -- Metadata
    c.dbt_updated_at

FROM categories c
LEFT JOIN products p ON c.category_key = p.product_category
