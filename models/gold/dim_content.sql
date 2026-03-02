{{
    config(
        materialized='table',
        schema='gold'
    )
}}

-- ============================================================================
-- DIMENSION: Content
-- ============================================================================
-- Purpose: Content catalog dimension with attributes
-- Layer: Gold
-- Type: Type 1 SCD (slowly changing dimension)
-- ============================================================================

with content as (
    select * from {{ ref('stg_content') }}
),

final as (
    select
        content_id,
        title,
        genre,
        duration_minutes,
        release_year,
        rating,
        content_type,
        
        -- Derived attributes
        case 
            when duration_minutes < 30 then 'Short'
            when duration_minutes between 30 and 90 then 'Medium'
            when duration_minutes > 90 then 'Long'
        end as duration_category,
        
        case
            when release_year >= year(current_date()) - 1 then 'New Release'
            when release_year >= year(current_date()) - 3 then 'Recent'
            else 'Catalog'
        end as release_recency,
        
        -- Metadata
        dbt_loaded_at,
        current_timestamp() as dbt_updated_at
        
    from content
)

select * from final
