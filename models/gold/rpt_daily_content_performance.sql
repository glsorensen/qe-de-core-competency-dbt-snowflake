{{
    config(
        materialized='table',
        schema='gold'
    )
}}

-- ============================================================================
-- REPORT/MART: Daily Content Performance
-- ============================================================================
-- Purpose: Aggregated daily metrics by content for analytics dashboards
-- Layer: Gold (Mart)
-- Grain: One row per content per date
-- ============================================================================

with viewership as (
    select * from {{ ref('fct_viewership') }}
),

content as (
    select * from {{ ref('dim_content') }}
),

dates as (
    select * from {{ ref('dim_date') }}
),

daily_content_metrics as (
    select
        -- Dimensions
        d.date_day,
        d.year,
        d.month,
        d.month_name,
        d.day_name,
        d.is_weekend,
        
        c.content_id,
        c.title,
        c.genre,
        c.content_type,
        c.rating,
        c.duration_category,
        c.release_recency,
        
        -- Viewership metrics
        count(distinct v.event_id) as total_views,
        count(distinct v.user_id) as unique_viewers,
        sum(v.watch_duration_minutes) as total_watch_minutes,
        avg(v.watch_duration_minutes) as avg_watch_minutes_per_view,
        
        -- Quality metrics
        sum(v.is_4k_viewing) as views_4k,
        sum(v.smooth_playback_flag) as smooth_playback_views,
        avg(v.buffer_count) as avg_buffer_count,
        
        -- Completion metrics
        sum(case when v.watch_duration_minutes >= c.duration_minutes * 0.9 then 1 else 0 end) as completed_views,
        avg(v.watch_duration_minutes / nullif(c.duration_minutes, 0)) as avg_completion_rate,
        
        -- Metadata
        current_timestamp() as dbt_updated_at
        
    from viewership v
    inner join content c on v.content_id = c.content_id
    inner join dates d on v.date_key = d.date_day
    
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)

select * from daily_content_metrics
