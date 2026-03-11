{{ config(
    materialized='view',
    tags=['silver', 'staging']
) }}

-- ============================================================================
-- SILVER LAYER - STAGING CUSTOMERS
-- ============================================================================
-- Transforms raw customer data into clean, standardized format
--
-- Transformations applied:
-- - Name and email standardization
-- - Phone number formatting
-- - Data quality filters
-- ============================================================================

WITH source_data AS (
    SELECT 
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        address,
        city,
        state,
        zip_code,
        created_at
    FROM {{ source('raw', 'customers') }}
)

SELECT 
    customer_id,
    
    -- Name standardization
    TRIM(INITCAP(first_name)) AS first_name,
    TRIM(INITCAP(last_name)) AS last_name,
    TRIM(LOWER(email)) AS email,
    
    -- Phone formatting
    phone,
    
    -- Address fields
    address,
    city,
    state,
    zip_code,
    
    -- Date fields
    created_at,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM source_data
WHERE customer_id IS NOT NULL
  AND email IS NOT NULL
  AND created_at IS NOT NULL
