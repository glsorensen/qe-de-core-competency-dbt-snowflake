{{
  config(
    materialized='table',
    tags=['gold', 'reviews', 'reports']
  )
}}

-- ============================================================================
-- REPORT: PRODUCT RATINGS
-- ============================================================================
-- Aggregated product review metrics for understanding product satisfaction
--
-- Business Logic:
-- - Calculates average, min, and max ratings per product
-- - Tracks total review count and positive review percentage
-- - Joins product details for complete context
-- ============================================================================

WITH reviews AS (
    SELECT * FROM {{ ref('fct_reviews') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

aggregated AS (
    SELECT
        r.product_id,
        COUNT(*) AS total_reviews,
        ROUND(AVG(r.rating), 2) AS avg_rating,
        MIN(r.rating) AS min_rating,
        MAX(r.rating) AS max_rating,
        SUM(CASE WHEN r.rating >= 4 THEN 1 ELSE 0 END) AS positive_review_count,
        ROUND(
            SUM(CASE WHEN r.rating >= 4 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0),
            1
        ) AS positive_review_pct

    FROM reviews r
    GROUP BY r.product_id
),

final AS (
    SELECT
        p.product_id,
        p.product_name,
        p.product_category,
        p.brand,
        p.price,
        a.total_reviews,
        a.avg_rating,
        a.min_rating,
        a.max_rating,
        a.positive_review_count,
        a.positive_review_pct

    FROM aggregated a
    LEFT JOIN products p ON a.product_id = p.product_id
)

SELECT * FROM final
ORDER BY avg_rating DESC, total_reviews DESC
