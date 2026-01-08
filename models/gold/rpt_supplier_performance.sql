{{
    config(
        materialized='table',
        tags=['report', 'suppliers', 'exercise5']
    )
}}

/*
    Supplier Performance Report
    ===========================
    Comprehensive supplier rankings and performance metrics.
    
    Business Logic:
    - Uses RANK() window function for supplier rankings
    - Compares supplier metrics to category and overall averages
    - Tracks product coverage and reliability
    - Identifies top and underperforming suppliers
    
    Grain: One row per supplier
*/

WITH supplier_base AS (
    SELECT *
    FROM {{ ref('dim_suppliers') }}
    WHERE is_active_contract = TRUE
),

-- Calculate average metrics for comparison
supplier_averages AS (
    SELECT
        AVG(reliability_score) AS avg_reliability_score,
        AVG(lead_time_days) AS avg_lead_time_days,
        AVG(total_products_supplied) AS avg_products_supplied,
        AVG(avg_margin_percentage) AS avg_margin_percentage
    FROM supplier_base
),

-- Rank suppliers by various metrics
supplier_rankings AS (
    SELECT
        s.*,
        
        -- Rankings using RANK() window function
        RANK() OVER (ORDER BY s.reliability_score DESC) AS reliability_rank,
        RANK() OVER (ORDER BY s.lead_time_days ASC) AS lead_time_rank,
        RANK() OVER (ORDER BY s.total_products_supplied DESC) AS product_coverage_rank,
        RANK() OVER (ORDER BY s.avg_margin_percentage DESC) AS margin_rank,
        
        -- Calculate composite score (weighted average of normalized metrics)
        ROUND(
            (s.reliability_score * 100 * 0.40) +  -- 40% weight on reliability
            ((1 - (s.lead_time_days / NULLIF(20, 0))) * 100 * 0.30) +  -- 30% weight on lead time (inverted, max 20 days)
            ((s.total_products_supplied / NULLIF(sa.avg_products_supplied, 0)) * 100 * 0.20) +  -- 20% weight on coverage
            ((s.avg_margin_percentage / NULLIF(sa.avg_margin_percentage, 0)) * 100 * 0.10),  -- 10% weight on margin
        2) AS composite_performance_score,
        
        -- Comparison to averages
        ROUND(s.reliability_score - sa.avg_reliability_score, 3) AS reliability_vs_avg,
        ROUND(s.lead_time_days - sa.avg_lead_time_days, 1) AS lead_time_vs_avg,
        s.total_products_supplied - ROUND(sa.avg_products_supplied, 0) AS products_vs_avg,
        ROUND(s.avg_margin_percentage - sa.avg_margin_percentage, 2) AS margin_vs_avg
        
    FROM supplier_base s
    CROSS JOIN supplier_averages sa
),

-- Add overall performance ranking based on composite score
final_rankings AS (
    SELECT
        *,
        RANK() OVER (ORDER BY composite_performance_score DESC) AS overall_performance_rank,
        
        -- Performance category
        CASE
            WHEN composite_performance_score >= 90 THEN 'Excellent'
            WHEN composite_performance_score >= 75 THEN 'Good'
            WHEN composite_performance_score >= 60 THEN 'Average'
            WHEN composite_performance_score >= 45 THEN 'Below Average'
            ELSE 'Poor'
        END AS performance_category,
        
        -- Highlight strengths and weaknesses
        CASE
            WHEN reliability_rank <= 2 THEN 'High Reliability'
            WHEN lead_time_rank <= 2 THEN 'Fast Delivery'
            WHEN product_coverage_rank <= 2 THEN 'Wide Product Range'
            WHEN margin_rank <= 2 THEN 'High Margins'
            ELSE NULL
        END AS key_strength,
        
        CASE
            WHEN reliability_rank >= (SELECT COUNT(*) FROM supplier_rankings) - 1 THEN 'Low Reliability'
            WHEN lead_time_rank >= (SELECT COUNT(*) FROM supplier_rankings) - 1 THEN 'Slow Delivery'
            WHEN low_reliability_flag THEN 'Reliability Concerns'
            WHEN contract_renewal_needed_flag THEN 'Contract Expiring Soon'
            WHEN data_quality_issues_count > 0 THEN 'Data Quality Issues'
            ELSE NULL
        END AS key_weakness,
        
        -- Action recommendations
        CASE
            WHEN contract_renewal_needed_flag THEN 'Review contract renewal'
            WHEN low_reliability_flag THEN 'Investigate reliability issues'
            WHEN lead_time_days > 14 AND lead_time_rank > 5 THEN 'Consider faster alternative'
            WHEN composite_performance_score < 50 THEN 'Review supplier relationship'
            WHEN composite_performance_score >= 85 THEN 'Expand partnership opportunities'
            ELSE 'Continue monitoring'
        END AS recommended_action
        
    FROM supplier_rankings
)

SELECT
    -- Supplier identification
    supplier_id,
    supplier_name,
    supplier_tier,
    country,
    
    -- Performance scores and rankings
    overall_performance_rank,
    composite_performance_score,
    performance_category,
    
    -- Individual metric rankings
    reliability_rank,
    lead_time_rank,
    product_coverage_rank,
    margin_rank,
    
    -- Key metrics
    reliability_score,
    lead_time_days,
    lead_time_category,
    total_products_supplied,
    primary_products_count,
    avg_margin_percentage,
    
    -- Comparison to averages
    reliability_vs_avg,
    lead_time_vs_avg,
    products_vs_avg,
    margin_vs_avg,
    
    -- Qualitative assessments
    key_strength,
    key_weakness,
    recommended_action,
    
    -- Risk flags
    is_preferred,
    is_active_contract,
    contract_days_remaining,
    contract_renewal_needed_flag,
    low_reliability_flag,
    data_quality_issues_count,
    
    -- Financial details
    payment_terms_days,
    avg_unit_cost,
    min_unit_cost,
    max_unit_cost,
    
    -- Time-based metrics
    days_as_supplier,
    
    -- Audit timestamp
    CURRENT_TIMESTAMP() AS dbt_updated_at
    
FROM final_rankings
ORDER BY overall_performance_rank
