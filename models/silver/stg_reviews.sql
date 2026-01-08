{{
  config(
    materialized='view',
    tags=['silver', 'reviews']
  )
}}

-- ============================================================================
-- STAGING: REVIEWS
-- ============================================================================
-- Cleans and standardizes customer review data from the raw layer
-- 
-- Transformations:
-- - Cast review_date to proper DATE type
-- - Trim whitespace from text fields
-- - Add calculated field: is_positive_review (rating >= 4)
-- - Add calculated field: review_length (character count)
-- ============================================================================

WITH source AS (
    SELECT * FROM {{ source('raw', 'reviews') }}
),

transformed AS (
    SELECT
        review_id,
        product_id,
        customer_id,
        rating::INTEGER AS rating,
        TRIM(review_title) AS review_title,
        TRIM(review_text) AS review_text,
        review_date::DATE AS review_date,
        
        -- Calculated fields
        rating >= 4 AS is_positive_review,
        LENGTH(TRIM(review_text)) AS review_length

    FROM source
)

SELECT * FROM transformed
