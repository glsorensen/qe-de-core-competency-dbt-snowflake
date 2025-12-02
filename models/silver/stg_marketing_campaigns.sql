{{ config(
    materialized='view',
    tags=['silver', 'staging']
) }}

-- ============================================================================
-- SILVER LAYER - STAGING MARKETING CAMPAIGNS
-- ============================================================================
-- Transforms raw marketing campaign data into clean, standardized format
--
-- Transformations applied:
-- - Campaign type standardization
-- - Calculate KPIs (CTR, CPA, ROAS)
-- - Date validation and duration calculation
-- - Budget tracking
-- ============================================================================

WITH source_data AS (
    SELECT
        campaign_id,
        campaign_name,
        campaign_type,
        start_date,
        end_date,
        budget,
        spend,
        impressions,
        clicks,
        conversions,
        created_at
    FROM {{ source('raw', 'marketing_campaigns') }}
),

cleaned AS (
    SELECT
        -- Primary key
        campaign_id,

        -- Campaign details
        TRIM(campaign_name) AS campaign_name,
        LOWER(TRIM(campaign_type)) AS campaign_type,

        -- Date fields
        start_date::DATE AS start_date,
        end_date::DATE AS end_date,

        -- Calculate campaign duration
        DATEDIFF('day', start_date, end_date) AS campaign_duration_days,

        -- Financial metrics
        ROUND(COALESCE(budget, 0), 2) AS budget,
        ROUND(COALESCE(spend, 0), 2) AS spend,

        -- Calculate budget utilization percentage
        CASE
            WHEN COALESCE(budget, 0) > 0 THEN
                ROUND((COALESCE(spend, 0) / budget) * 100, 2)
            ELSE 0
        END AS budget_utilization_pct,

        -- Performance metrics
        COALESCE(impressions, 0) AS impressions,
        COALESCE(clicks, 0) AS clicks,
        COALESCE(conversions, 0) AS conversions,

        -- Calculate Click-Through Rate (CTR)
        CASE
            WHEN COALESCE(impressions, 0) > 0 THEN
                ROUND((COALESCE(clicks, 0)::FLOAT / impressions) * 100, 4)
            ELSE 0
        END AS ctr_percentage,

        -- Calculate Conversion Rate
        CASE
            WHEN COALESCE(clicks, 0) > 0 THEN
                ROUND((COALESCE(conversions, 0)::FLOAT / clicks) * 100, 4)
            ELSE 0
        END AS conversion_rate_percentage,

        -- Calculate Cost Per Click (CPC)
        CASE
            WHEN COALESCE(clicks, 0) > 0 THEN
                ROUND(COALESCE(spend, 0) / clicks, 2)
            ELSE 0
        END AS cost_per_click,

        -- Calculate Cost Per Acquisition (CPA)
        CASE
            WHEN COALESCE(conversions, 0) > 0 THEN
                ROUND(COALESCE(spend, 0) / conversions, 2)
            ELSE 0
        END AS cost_per_acquisition,

        -- Campaign status flags
        CASE
            WHEN CURRENT_DATE() < start_date THEN 'upcoming'
            WHEN CURRENT_DATE() BETWEEN start_date AND end_date THEN 'active'
            WHEN CURRENT_DATE() > end_date THEN 'completed'
            ELSE 'unknown'
        END AS campaign_status,

        CASE
            WHEN spend > budget THEN TRUE
            ELSE FALSE
        END AS is_over_budget,

        CASE
            WHEN conversions > 0 THEN TRUE
            ELSE FALSE
        END AS has_conversions,

        -- Timestamps
        created_at,

        -- Metadata
        CURRENT_TIMESTAMP() AS dbt_updated_at

    FROM source_data
)

SELECT *
FROM cleaned
WHERE
    -- Data quality filters
    campaign_id IS NOT NULL
    AND campaign_name IS NOT NULL
    AND start_date IS NOT NULL
    AND end_date IS NOT NULL
    AND start_date <= end_date  -- Start date must be before or equal to end date
    AND campaign_type IN ('email', 'social', 'search', 'display', 'affiliate', 'influencer', 'other')
