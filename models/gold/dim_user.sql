{{
    config(
        materialized='table',
        schema='gold'
    )
}}

-- ============================================================================
-- DIMENSION: User
-- ============================================================================
-- Purpose: User demographic dimension
-- Layer: Gold
-- Type: Type 1 SCD
-- ============================================================================

with users as (
    select * from {{ ref('stg_users') }}
),

final as (
    select
        user_id,
        signup_date,
        country,
        subscription_tier,
        age_group,
        days_since_signup,
        
        -- Derived attributes
        case
            when days_since_signup < 30 then 'New User'
            when days_since_signup < 90 then 'Active User'
            when days_since_signup < 365 then 'Established User'
            else 'Loyal User'
        end as user_tenure_segment,
        
        case
            when subscription_tier = 'premium' then 'High Value'
            when subscription_tier = 'basic' then 'Medium Value'
            else 'Free Tier'
        end as value_segment,
        
        -- Metadata
        dbt_loaded_at,
        current_timestamp() as dbt_updated_at
        
    from users
)

select * from final
