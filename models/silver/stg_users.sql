{{
    config(
        materialized='view',
        schema='silver'
    )
}}

-- ============================================================================
-- STAGING MODEL: Users
-- ============================================================================
-- Purpose: Clean and standardize user demographic data
-- Layer: Silver (Staging)
-- ============================================================================

with source as (
    select * from {{ source('media_raw', 'raw_users') }}
),

cleaned as (
    select
        -- Primary key
        user_id,
        
        -- Dates
        signup_date,
        
        -- Dimensions
        upper(country) as country,
        lower(subscription_tier) as subscription_tier,
        age_group,
        
        -- Derived attributes
        datediff('day', signup_date, current_date()) as days_since_signup,
        
        -- Metadata
        current_timestamp() as dbt_loaded_at
        
    from source
)

select * from cleaned
