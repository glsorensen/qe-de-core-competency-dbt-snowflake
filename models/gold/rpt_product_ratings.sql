{{
    config(
        materialized='table'
    )
}}

-- ============================================================================
-- GOLD LAYER - PRODUCT RATINGS REPORT
-- ============================================================================
-- Aggregated product rating metrics for product performance analysis
-- ============================================================================

SELECT
    p.product_id,
    p.product_name,
    p.product_category,
    p.brand,
    
    -- Review metrics
    COUNT(*) AS total_reviews,
    ROUND(AVG(r.rating), 2) AS avg_rating,
    MIN(r.rating) AS min_rating,
    MAX(r.rating) AS max_rating,
    
    -- Positive review metrics
    SUM(CASE WHEN r.rating >= 4 THEN 1 ELSE 0 END) AS positive_review_count,
    ROUND(
        SUM(CASE WHEN r.rating >= 4 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0),
        1
    ) AS positive_review_pct,
    
    -- Additional insights
    MIN(r.review_date) AS first_review_date,
    MAX(r.review_date) AS last_review_date

FROM {{ ref('fct_reviews') }} r
LEFT JOIN {{ ref('stg_products') }} p 
    ON r.product_id = p.product_id

GROUP BY
    p.product_id,
    p.product_name,
    p.product_category,
    p.brand

ORDER BY avg_rating DESC
