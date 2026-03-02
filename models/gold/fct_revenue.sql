{{
    config(
        materialized='table',
        schema='gold'
    )
}}

-- ============================================================================
-- FACT TABLE: Revenue Transactions
-- ============================================================================
-- Purpose: Fact table for all revenue transactions
-- Layer: Gold
-- Grain: One row per transaction
-- ============================================================================

with revenue as (
    select * from {{ ref('stg_revenue') }}
),

users as (
    select user_id from {{ ref('dim_user') }}
),

dates as (
    select date_day from {{ ref('dim_date') }}
),

final as (
    select
        -- Surrogate key
        r.transaction_id,
        
        -- Foreign keys
        r.user_id,
        r.transaction_date as date_key,
        
        -- Degenerate dimensions
        r.transaction_timestamp,
        r.transaction_type,
        r.payment_method,
        r.currency,
        
        -- Metrics
        r.amount_usd,
        
        -- Derived metrics
        case 
            when r.transaction_type = 'subscription' then 1 
            else 0 
        end as is_recurring_revenue,
        
        case
            when r.transaction_type in ('rental', 'purchase') then 1
            else 0
        end as is_transactional_revenue,
        
        case
            when r.transaction_type = 'ad_revenue' then 1
            else 0
        end as is_ad_revenue,
        
        -- Metadata
        r.dbt_loaded_at,
        current_timestamp() as dbt_updated_at
        
    from revenue r
    inner join users u on r.user_id = u.user_id
    inner join dates d on r.transaction_date = d.date_day
)

select * from final
