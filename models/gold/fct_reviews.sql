{{
    config(
        materialized='table'
    )
}}

-- ============================================================================
-- GOLD LAYER - FACT REVIEWS
-- ============================================================================
-- Review fact table with enriched metrics for analysis
-- ============================================================================

SELECT
    -- Primary key
    review_id,
    
    -- Foreign keys
    product_id,
    customer_id,
    
    -- Review attributes
    rating,
    review_title,
    review_text,
    review_date,
    
    -- Inherited calculated fields
    is_positive_review,
    review_length,
    
    -- Additional calculated fields
    DATEDIFF('day', review_date, CURRENT_DATE) AS review_age_days,
    
    CASE
        WHEN rating <= 2 THEN 'Poor'
        WHEN rating = 3 THEN 'Average'
        ELSE 'Good'
    END AS rating_category

FROM {{ ref('stg_reviews') }}
