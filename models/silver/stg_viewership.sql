{{
    config(
        materialized='view',
        schema='silver'
    )
}}

-- ============================================================================
-- STAGING MODEL: Viewership Events
-- ============================================================================
-- Purpose: Clean and standardize raw viewership data
-- Layer: Silver (Staging)
-- Materialization: View (lightweight transformation)
-- ============================================================================

with source as (
    select * from {{ source('media_raw', 'raw_viewership') }}
),

cleaned as (
    select
        -- Primary keys
        event_id,
        user_id,
        content_id,
        
        -- Timestamps
        event_timestamp,
        date(event_timestamp) as event_date,
        
        -- Metrics
        watch_duration_seconds,
        round(watch_duration_seconds / 60.0, 2) as watch_duration_minutes,
        
        -- Dimensions
        lower(device_type) as device_type,
        upper(quality) as quality,
        buffer_count,
        
        -- Metadata
        current_timestamp() as dbt_loaded_at
        
    from source
)

select * from cleaned
