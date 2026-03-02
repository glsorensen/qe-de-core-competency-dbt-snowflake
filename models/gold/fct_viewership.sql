{{
    config(
        materialized='table',
        schema='gold'
    )
}}

-- ============================================================================
-- FACT TABLE: Viewership Events
-- ============================================================================
-- Purpose: Fact table for all viewership events with metrics
-- Layer: Gold
-- Grain: One row per viewing event
-- ============================================================================

with viewership as (
    select * from {{ ref('stg_viewership') }}
),

users as (
    select user_id from {{ ref('dim_user') }}
),

content as (
    select content_id from {{ ref('dim_content') }}
),

dates as (
    select date_day from {{ ref('dim_date') }}
),

final as (
    select
        -- Surrogate key
        v.event_id,
        
        -- Foreign keys
        v.user_id,
        v.content_id,
        v.event_date as date_key,
        
        -- Degenerate dimensions
        v.event_timestamp,
        v.device_type,
        v.quality,
        
        -- Metrics
        v.watch_duration_seconds,
        v.watch_duration_minutes,
        v.buffer_count,
        
        -- Derived metrics
        case when v.buffer_count = 0 then 1 else 0 end as smooth_playback_flag,
        case when v.quality = '4K' then 1 else 0 end as is_4k_viewing,
        
        -- Metadata
        v.dbt_loaded_at,
        current_timestamp() as dbt_updated_at
        
    from viewership v
    inner join users u on v.user_id = u.user_id
    inner join content c on v.content_id = c.content_id
    inner join dates d on v.event_date = d.date_day
)

select * from final
