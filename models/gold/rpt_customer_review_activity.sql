{{
    config(
        materialized='table'
    )
}}

-- ============================================================================
-- GOLD LAYER - CUSTOMER REVIEW ACTIVITY REPORT
-- ============================================================================
-- Customer review activity metrics and reviewer classification
-- ============================================================================

SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name AS full_name,
    c.email,
    
    -- Review activity metrics
    COUNT(r.review_id) AS total_reviews_written,
    ROUND(AVG(r.rating), 2) AS avg_rating_given,
    MIN(r.review_date) AS first_review_date,
    MAX(r.review_date) AS last_review_date,
    DATEDIFF('day', MAX(r.review_date), CURRENT_DATE) AS days_since_last_review,
    
    -- Reviewer classification
    CASE
        WHEN AVG(r.rating) < 3 THEN 'Harsh Critic'
        WHEN AVG(r.rating) >= 4 THEN 'Enthusiastic Reviewer'
        ELSE 'Balanced Reviewer'
    END AS reviewer_type

FROM {{ ref('stg_customers') }} c
LEFT JOIN {{ ref('fct_reviews') }} r 
    ON c.customer_id = r.customer_id

GROUP BY
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email

HAVING COUNT(r.review_id) > 0  -- Only include customers who have written reviews

ORDER BY total_reviews_written DESC
