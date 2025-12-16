{{ config(materialized='table') }}

-- =============================================================================
-- GOLD LAYER - Supplier Performance Report
-- =============================================================================
-- Comprehensive supplier performance metrics with rankings
-- =============================================================================

WITH suppliers AS (
    SELECT * FROM {{ ref('dim_suppliers') }}
),

product_suppliers AS (
    SELECT * FROM {{ ref('stg_product_suppliers') }}
),

-- Get inventory value per supplier (based on current inventory for their products)
inventory_by_supplier AS (
    SELECT
        ps.supplier_id,
        SUM(ih.quantity_on_hand * ps.unit_cost) AS total_inventory_value,
        COUNT(DISTINCT ps.product_id) AS products_in_stock,
        SUM(CASE WHEN ih.is_stockout THEN 1 ELSE 0 END) AS stockout_count
    FROM product_suppliers ps
    INNER JOIN {{ ref('dim_inventory_history') }} ih ON ps.product_id = ih.product_id
    WHERE ih.is_current = TRUE
      AND ps.is_primary_supplier = TRUE
    GROUP BY ps.supplier_id
),

-- Get transaction volume per supplier (restocks)
restock_metrics AS (
    SELECT
        ps.supplier_id,
        COUNT(*) AS restock_count,
        SUM(t.quantity) AS total_units_restocked,
        SUM(t.transaction_value) AS total_restock_value,
        AVG(t.transaction_value) AS avg_restock_value
    FROM {{ ref('stg_inventory_transactions') }} t
    INNER JOIN product_suppliers ps ON t.product_id = ps.product_id
    WHERE t.transaction_type = 'restock'
      AND ps.is_primary_supplier = TRUE
    GROUP BY ps.supplier_id
),

final AS (
    SELECT
        -- Supplier identification
        s.supplier_id,
        s.supplier_name,
        s.country,
        s.supplier_tier,

        -- Supplier attributes
        s.lead_time_days,
        s.lead_time_category,
        s.reliability_score,
        s.payment_terms_days,
        s.is_preferred,
        s.is_active_contract,
        s.days_as_supplier,

        -- Product metrics
        s.total_products_supplied,
        s.primary_product_count,
        s.avg_margin_percentage,
        s.avg_unit_cost,

        -- Inventory metrics
        COALESCE(ibs.total_inventory_value, 0) AS total_inventory_value,
        COALESCE(ibs.products_in_stock, 0) AS products_in_stock,
        COALESCE(ibs.stockout_count, 0) AS stockout_count,

        -- Restock metrics
        COALESCE(rm.restock_count, 0) AS restock_count,
        COALESCE(rm.total_units_restocked, 0) AS total_units_restocked,
        COALESCE(rm.total_restock_value, 0) AS total_restock_value,
        ROUND(COALESCE(rm.avg_restock_value, 0), 2) AS avg_restock_value,

        -- Performance scores
        -- On-time delivery score (using reliability as proxy)
        ROUND(s.reliability_score * 100, 1) AS on_time_delivery_score,

        -- Supply chain risk score (lower is better)
        CASE
            WHEN ibs.stockout_count > 2 THEN 'high'
            WHEN ibs.stockout_count > 0 THEN 'medium'
            ELSE 'low'
        END AS supply_chain_risk,

        -- Rankings
        RANK() OVER (ORDER BY COALESCE(ibs.total_inventory_value, 0) DESC) AS value_rank,
        RANK() OVER (ORDER BY s.reliability_score DESC) AS reliability_rank,
        RANK() OVER (ORDER BY s.avg_margin_percentage DESC NULLS LAST) AS margin_rank,
        RANK() OVER (ORDER BY COALESCE(rm.restock_count, 0) DESC) AS activity_rank,

        -- Comparison to averages
        ROUND(s.avg_margin_percentage - AVG(s.avg_margin_percentage) OVER (), 2) AS margin_vs_average,
        ROUND(s.reliability_score - AVG(s.reliability_score) OVER (), 3) AS reliability_vs_average,
        ROUND(COALESCE(ibs.total_inventory_value, 0) - AVG(COALESCE(ibs.total_inventory_value, 0)) OVER (), 2) AS value_vs_average,

        -- Overall supplier score (weighted composite)
        ROUND(
            (s.reliability_score * 0.3) +
            (COALESCE(s.avg_margin_percentage, 0) / 100 * 0.3) +
            (CASE WHEN s.lead_time_category = 'fast' THEN 1 WHEN s.lead_time_category = 'medium' THEN 0.7 ELSE 0.4 END * 0.2) +
            (CASE WHEN s.is_preferred THEN 1 ELSE 0.5 END * 0.2),
            3
        ) AS overall_supplier_score,

        -- Metadata
        CURRENT_TIMESTAMP AS report_generated_at

    FROM suppliers s
    LEFT JOIN inventory_by_supplier ibs ON s.supplier_id = ibs.supplier_id
    LEFT JOIN restock_metrics rm ON s.supplier_id = rm.supplier_id
)

SELECT * FROM final
ORDER BY overall_supplier_score DESC
