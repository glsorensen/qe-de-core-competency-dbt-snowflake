{{
  config(
    materialized='table',
    tags=['gold', 'reviews', 'reports']
  )
}}

-- ============================================================================
-- REPORT: CUSTOMER REVIEW ACTIVITY
-- ============================================================================
-- Analyzes customer review behavior and engagement patterns
--
-- Business Logic:
-- - Tracks review volume and rating tendencies per customer
-- - Identifies first and most recent review dates
-- - Classifies reviewers as "Harsh Critic", "Balanced", or "Enthusiastic"
-- ============================================================================

WITH reviews AS (
    SELECT * FROM {{ ref('fct_reviews') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

aggregated AS (
    SELECT
        r.customer_id,
        COUNT(*) AS total_reviews_written,
        ROUND(AVG(r.rating), 2) AS avg_rating_given,
        MIN(r.review_date) AS first_review_date,
        MAX(r.review_date) AS last_review_date,
        DATEDIFF('day', MAX(r.review_date), CURRENT_DATE) AS days_since_last_review,
        
        -- Classify reviewer type based on average rating
        CASE
            WHEN AVG(r.rating) < 3 THEN 'Harsh Critic'
            WHEN AVG(r.rating) >= 4 THEN 'Enthusiastic Reviewer'
            ELSE 'Balanced Reviewer'
        END AS reviewer_type

    FROM reviews r
    GROUP BY r.customer_id
),

final AS (
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        a.total_reviews_written,
        a.avg_rating_given,
        a.first_review_date,
        a.last_review_date,
        a.days_since_last_review,
        a.reviewer_type

    FROM aggregated a
    LEFT JOIN customers c ON a.customer_id = c.customer_id
)

SELECT * FROM final
ORDER BY total_reviews_written DESC, avg_rating_given DESC
