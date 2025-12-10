{{
    config(
        materialized='view'
    )
}}

-- ============================================================================
-- SILVER LAYER - STAGING REVIEWS
-- ============================================================================
-- Cleaned and standardized review data with calculated fields
-- ============================================================================

SELECT
    -- Primary key
    review_id,
    
    -- Foreign keys
    product_id,
    customer_id,
    
    -- Rating (ensure integer)
    rating::INTEGER AS rating,
    
    -- Text fields (trimmed)
    TRIM(review_title) AS review_title,
    TRIM(review_text) AS review_text,
    
    -- Date field (cast to proper DATE type)
    review_date::DATE AS review_date,
    
    -- Calculated fields
    rating >= 4 AS is_positive_review,
    LENGTH(TRIM(review_text)) AS review_length

FROM {{ source('raw', 'reviews') }}
