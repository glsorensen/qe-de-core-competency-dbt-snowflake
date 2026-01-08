{{
    config(
        materialized='table',
        tags=['dimension', 'suppliers', 'exercise5']
    )
}}

/*
    Supplier Dimension Table
    ========================
    Enriched supplier dimension with aggregated metrics and tier classification.
    
    Business Logic:
    - Supplier tier based on reliability score and preferred status
    - Aggregated product and cost metrics from product_suppliers
    - Contract status and lead time categorization
    
    Grain: One row per supplier
*/

WITH supplier_base AS (
    SELECT *
    FROM {{ ref('stg_suppliers') }}
),

product_supplier_metrics AS (
    SELECT
        supplier_id,
        COUNT(DISTINCT product_id) AS total_products_supplied,
        COUNT(DISTINCT CASE WHEN is_primary_supplier THEN product_id END) AS primary_products_count,
        ROUND(AVG(unit_cost), 2) AS avg_unit_cost,
        ROUND(MIN(unit_cost), 2) AS min_unit_cost,
        ROUND(MAX(unit_cost), 2) AS max_unit_cost,
        ROUND(AVG(margin_percentage), 2) AS avg_margin_percentage,
        SUM(CASE WHEN cost_exceeds_price_flag THEN 1 ELSE 0 END) AS data_quality_issues_count
    FROM {{ ref('stg_product_suppliers') }}
    GROUP BY supplier_id
),

final AS (
    SELECT
        -- Supplier identifiers
        s.supplier_id,
        s.supplier_name,
        s.contact_email,
        s.country,
        
        -- Lead time attributes
        s.lead_time_days,
        s.lead_time_category,
        
        -- Reliability and preferences
        s.reliability_score,
        s.is_preferred,
        
        -- Contract information
        s.contract_start_date,
        s.contract_end_date,
        s.is_active_contract,
        s.contract_days_remaining,
        s.days_as_supplier,
        
        -- Payment terms
        s.payment_terms_days,
        
        -- Product metrics (enriched from product_suppliers)
        COALESCE(psm.total_products_supplied, 0) AS total_products_supplied,
        COALESCE(psm.primary_products_count, 0) AS primary_products_count,
        psm.avg_unit_cost,
        psm.min_unit_cost,
        psm.max_unit_cost,
        psm.avg_margin_percentage,
        COALESCE(psm.data_quality_issues_count, 0) AS data_quality_issues_count,
        
        -- Supplier tier classification
        CASE
            WHEN s.reliability_score >= 0.95 AND s.is_preferred AND s.lead_time_category = 'fast' THEN 'Platinum'
            WHEN s.reliability_score >= 0.90 AND s.is_preferred THEN 'Gold'
            WHEN s.reliability_score >= 0.85 OR s.is_preferred THEN 'Silver'
            ELSE 'Bronze'
        END AS supplier_tier,
        
        -- Risk flags
        CASE WHEN NOT s.is_active_contract THEN TRUE ELSE FALSE END AS contract_expiring_soon_flag,
        CASE WHEN s.contract_days_remaining IS NOT NULL AND s.contract_days_remaining <= 90 THEN TRUE ELSE FALSE END AS contract_renewal_needed_flag,
        CASE WHEN s.reliability_score < 0.80 THEN TRUE ELSE FALSE END AS low_reliability_flag,
        
        -- Audit fields
        CURRENT_TIMESTAMP() AS dbt_updated_at
    FROM supplier_base s
    LEFT JOIN product_supplier_metrics psm
        ON s.supplier_id = psm.supplier_id
)

SELECT * FROM final
