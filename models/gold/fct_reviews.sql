{{
  config(
    materialized='table',
    tags=['gold', 'reviews', 'facts']
  )
}}

-- ============================================================================
-- FACT TABLE: REVIEWS
-- ============================================================================
-- Customer product reviews with additional calculated metrics
--
-- Business Logic:
-- - review_age_days: Days since review was posted (for recency analysis)
-- - rating_category: Poor (1-2), Average (3), Good (4-5)
-- ============================================================================

WITH reviews AS (
    SELECT * FROM {{ ref('stg_reviews') }}
),

final AS (
    SELECT
        review_id,
        product_id,
        customer_id,
        rating,
        review_title,
        review_text,
        review_date,
        is_positive_review,
        review_length,
        
        -- Days since review was posted
        DATEDIFF('day', review_date, CURRENT_DATE) AS review_age_days,
        
        -- Categorize rating level
        CASE
            WHEN rating <= 2 THEN 'Poor'
            WHEN rating = 3 THEN 'Average'
            WHEN rating >= 4 THEN 'Good'
        END AS rating_category

    FROM reviews
)

SELECT * FROM final
