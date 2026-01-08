{{ config(
    materialized='table',
    tags=['gold', 'dimension']
) }}

-- ============================================================================
-- GOLD LAYER - CATEGORY DIMENSION
-- ============================================================================
-- Business-ready category dimension table
--
-- Features:
-- - Complete category information
-- - Department groupings
-- - Category age tracking
-- ============================================================================

SELECT
    -- Primary key
    category_id,
    
    -- Category attributes
    category_name,
    department,
    
    -- Dates
    created_at AS category_created_at,
    days_since_created AS category_age_days,
    
    -- Metadata
    dbt_updated_at

FROM {{ ref('stg_categories') }}
