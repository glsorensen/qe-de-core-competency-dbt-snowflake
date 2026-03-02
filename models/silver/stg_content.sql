{{
    config(
        materialized='view',
        schema='silver'
    )
}}

-- ============================================================================
-- STAGING MODEL: Content Catalog
-- ============================================================================
-- Purpose: Clean and standardize content metadata
-- Layer: Silver (Staging)
-- ============================================================================

with source as (
    select * from {{ source('media_raw', 'raw_content') }}
),

cleaned as (
    select
        -- Primary key
        content_id,
        
        -- Attributes
        trim(title) as title,
        initcap(genre) as genre,
        duration_minutes,
        release_year,
        upper(rating) as rating,
        lower(content_type) as content_type,
        
        -- Metadata
        current_timestamp() as dbt_loaded_at
        
    from source
)

select * from cleaned
