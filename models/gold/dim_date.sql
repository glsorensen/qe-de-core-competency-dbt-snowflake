{{
    config(
        materialized='table',
        schema='gold'
    )
}}

-- ============================================================================
-- DIMENSION: Date
-- ============================================================================
-- Purpose: Date dimension for time-based analysis
-- Layer: Gold
-- ============================================================================

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-01-01' as date)",
        end_date="cast('2027-12-31' as date)"
    )
    }}
),

dates as (
    select
        date_day,
        year(date_day) as year,
        month(date_day) as month,
        monthname(date_day) as month_name,
        day(date_day) as day,
        dayname(date_day) as day_name,
        dayofweek(date_day) as day_of_week,
        weekofyear(date_day) as week_of_year,
        quarter(date_day) as quarter,
        case when dayofweek(date_day) in (0, 6) then true else false end as is_weekend
    from date_spine
)

select * from dates
