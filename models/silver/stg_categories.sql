{{ config(
    materialized='view',
    tags=['silver', 'staging']
) }}

-- ============================================================================
-- SILVER LAYER - STAGING CATEGORIES
-- ============================================================================
-- Transforms raw category data into clean, standardized format
--
-- Transformations applied:
-- - Name and department standardization
-- - Date field casting
-- - Data quality filters
-- ============================================================================

WITH source_data AS (
    SELECT
        category_id,
        category_name,
        department,
        created_at
    FROM {{ source('raw', 'categories') }}
)

SELECT
    -- Primary key
    category_id,
    
    -- Category attributes
    TRIM(INITCAP(category_name)) AS category_name,
    TRIM(INITCAP(department)) AS department,
    
    -- Date fields
    created_at::DATE AS created_at,
    
    -- Derived fields
    DATEDIFF('day', created_at, CURRENT_DATE()) AS days_since_created,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM source_data
WHERE category_id IS NOT NULL
  AND category_name IS NOT NULL
