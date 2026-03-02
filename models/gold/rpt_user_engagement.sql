{{
    config(
        materialized='table',
        schema='gold'
    )
}}

-- ============================================================================
-- REPORT/MART: User Engagement Summary
-- ============================================================================
-- Purpose: User-level engagement metrics combining viewership and revenue
-- Layer: Gold (Mart)
-- Grain: One row per user
-- ============================================================================

with viewership as (
    select * from {{ ref('fct_viewership') }}
),

revenue as (
    select * from {{ ref('fct_revenue') }}
),

users as (
    select * from {{ ref('dim_user') }}
),

user_viewership_metrics as (
    select
        user_id,
        count(distinct event_id) as total_views,
        count(distinct content_id) as unique_content_watched,
        count(distinct date_key) as active_days,
        sum(watch_duration_minutes) as total_watch_minutes,
        avg(watch_duration_minutes) as avg_watch_minutes_per_session,
        max(event_timestamp) as last_view_timestamp,
        min(event_timestamp) as first_view_timestamp
    from viewership
    group by 1
),

user_revenue_metrics as (
    select
        user_id,
        count(distinct transaction_id) as total_transactions,
        sum(amount_usd) as lifetime_value,
        sum(is_recurring_revenue * amount_usd) as recurring_revenue,
        sum(is_transactional_revenue * amount_usd) as transactional_revenue,
        max(transaction_timestamp) as last_transaction_timestamp
    from revenue
    group by 1
),

final as (
    select
        -- User dimensions
        u.user_id,
        u.country,
        u.subscription_tier,
        u.age_group,
        u.user_tenure_segment,
        u.value_segment,
        u.signup_date,
        u.days_since_signup,
        
        -- Viewership metrics
        coalesce(v.total_views, 0) as total_views,
        coalesce(v.unique_content_watched, 0) as unique_content_watched,
        coalesce(v.active_days, 0) as active_days,
        coalesce(v.total_watch_minutes, 0) as total_watch_minutes,
        coalesce(v.avg_watch_minutes_per_session, 0) as avg_watch_minutes_per_session,
        v.last_view_timestamp,
        v.first_view_timestamp,
        
        -- Revenue metrics
        coalesce(r.total_transactions, 0) as total_transactions,
        coalesce(r.lifetime_value, 0) as lifetime_value,
        coalesce(r.recurring_revenue, 0) as recurring_revenue,
        coalesce(r.transactional_revenue, 0) as transactional_revenue,
        r.last_transaction_timestamp,
        
        -- Engagement scores
        case
            when v.active_days >= 20 and v.total_watch_minutes > 1000 then 'Highly Engaged'
            when v.active_days >= 10 and v.total_watch_minutes > 500 then 'Engaged'
            when v.active_days >= 5 then 'Moderately Engaged'
            when v.active_days >= 1 then 'Low Engagement'
            else 'Inactive'
        end as engagement_level,
        
        -- Metadata
        current_timestamp() as dbt_updated_at
        
    from users u
    left join user_viewership_metrics v on u.user_id = v.user_id
    left join user_revenue_metrics r on u.user_id = r.user_id
)

select * from final
