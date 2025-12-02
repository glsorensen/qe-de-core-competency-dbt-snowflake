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
-- - Phone number cleansing
-- - Age and tenure calculations
-- - Data quality filters
-- ============================================================================

WITH source_data AS (
    SELECT 
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        date_of_birth,
        created_at,
        updated_at,
        customer_lifetime_value,
        acquisition_channel
    FROM {{ source('raw', 'customers') }}
)

SELECT 
    customer_id,
    
    -- Name standardization
    TRIM(INITCAP(first_name)) AS first_name,
    TRIM(INITCAP(last_name)) AS last_name,
    TRIM(LOWER(email)) AS email,
    
    -- Phone formatting (remove non-numeric characters)
    REGEXP_REPLACE(phone, '[^0-9]', '') AS phone_clean,
    
    -- Date fields
    date_of_birth,
    created_at,
    updated_at,
    
    -- Financial fields
    COALESCE(customer_lifetime_value, 0) AS customer_lifetime_value,
    
    -- Categorization
    LOWER(acquisition_channel) AS acquisition_channel,
    
    -- Derived fields
    DATEDIFF('year', date_of_birth, CURRENT_DATE()) AS customer_age,
    DATEDIFF('day', created_at, CURRENT_DATE()) AS days_since_signup,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM source_data
WHERE customer_id IS NOT NULL
  AND email IS NOT NULL
  AND created_at IS NOT NULL