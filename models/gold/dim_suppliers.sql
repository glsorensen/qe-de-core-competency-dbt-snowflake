{{ config(materialized='table') }}

-- =============================================================================
-- GOLD LAYER - Supplier Dimension
-- =============================================================================
-- Comprehensive supplier dimension with aggregated metrics and tiering
-- =============================================================================

WITH suppliers AS (
    SELECT * FROM {{ ref('stg_suppliers') }}
),

product_suppliers AS (
    SELECT * FROM {{ ref('stg_product_suppliers') }}
),

-- Aggregate metrics per supplier
supplier_metrics AS (
    SELECT
        supplier_id,
        COUNT(DISTINCT product_id) AS total_products_supplied,
        AVG(margin_percentage) AS avg_margin_percentage,
        SUM(CASE WHEN is_primary_supplier THEN 1 ELSE 0 END) AS primary_product_count,
        AVG(unit_cost) AS avg_unit_cost
    FROM product_suppliers
    GROUP BY supplier_id
),

final AS (
    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['s.supplier_id']) }} AS supplier_sk,

        -- Natural key
        s.supplier_id,

        -- Supplier attributes
        s.supplier_name,
        s.contact_email,
        s.country,
        s.lead_time_days,
        s.lead_time_category,
        s.reliability_score,
        s.payment_terms_days,
        s.is_preferred,

        -- Contract information
        s.contract_start_date,
        s.contract_end_date,
        s.is_active_contract,
        s.contract_days_remaining,

        -- Derived: Days as supplier
        DATEDIFF(day, s.contract_start_date, CURRENT_DATE) AS days_as_supplier,

        -- Aggregated metrics from product_suppliers
        COALESCE(sm.total_products_supplied, 0) AS total_products_supplied,
        ROUND(sm.avg_margin_percentage, 2) AS avg_margin_percentage,
        COALESCE(sm.primary_product_count, 0) AS primary_product_count,
        ROUND(sm.avg_unit_cost, 2) AS avg_unit_cost,

        -- Supplier tier classification
        CASE
            WHEN s.is_preferred = TRUE AND s.reliability_score > 0.9 THEN 'platinum'
            WHEN s.is_preferred = TRUE OR s.reliability_score > 0.85 THEN 'gold'
            ELSE 'silver'
        END AS supplier_tier,

        -- Metadata
        CURRENT_TIMESTAMP AS dbt_updated_at

    FROM suppliers s
    LEFT JOIN supplier_metrics sm ON s.supplier_id = sm.supplier_id
)

SELECT * FROM final
