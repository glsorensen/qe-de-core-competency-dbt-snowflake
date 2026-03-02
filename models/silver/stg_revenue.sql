{{
    config(
        materialized='view',
        schema='silver'
    )
}}

-- ============================================================================
-- STAGING MODEL: Revenue Transactions
-- ============================================================================
-- Purpose: Clean and standardize raw revenue data
-- Layer: Silver (Staging)
-- ============================================================================

with source as (
    select * from {{ source('media_raw', 'raw_revenue') }}
),

cleaned as (
    select
        -- Primary keys
        transaction_id,
        user_id,
        
        -- Timestamps
        transaction_timestamp,
        date(transaction_timestamp) as transaction_date,
        
        -- Metrics
        amount_usd,
        
        -- Dimensions
        lower(transaction_type) as transaction_type,
        lower(payment_method) as payment_method,
        upper(currency) as currency,
        
        -- Metadata
        current_timestamp() as dbt_loaded_at
        
    from source
)

select * from cleaned
