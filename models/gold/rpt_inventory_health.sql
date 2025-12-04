{{ config(materialized='table') }}

-- =============================================================================
-- GOLD LAYER - Inventory Health Report
-- =============================================================================
-- Comprehensive inventory health dashboard showing stockout risks,
-- days of supply, and reorder recommendations
-- =============================================================================

-- Get average daily sales velocity over the last 30 days
WITH daily_sales AS (
    SELECT
        product_id,
        SUM(ABS(quantity)) AS total_sold,
        COUNT(DISTINCT transaction_date) AS days_with_sales
    FROM {{ ref('stg_inventory_transactions') }}
    WHERE transaction_type = 'sale'
      AND transaction_date >= DATEADD(day, -30, CURRENT_DATE)
    GROUP BY product_id
),

avg_daily_sales AS (
    SELECT
        product_id,
        total_sold / 30.0 AS avg_daily_units,
        total_sold,
        days_with_sales
    FROM daily_sales
),

-- Get current inventory status (most recent snapshot for each product)
current_inventory AS (
    SELECT *
    FROM {{ ref('dim_inventory_history') }}
    WHERE is_current = TRUE
),

-- Get primary supplier info
primary_suppliers AS (
    SELECT
        ps.product_id,
        s.supplier_id,
        s.supplier_name,
        s.lead_time_days,
        s.lead_time_category,
        s.reliability_score,
        s.supplier_tier,
        ps.unit_cost,
        ps.minimum_order_quantity
    FROM {{ ref('stg_product_suppliers') }} ps
    INNER JOIN {{ ref('dim_suppliers') }} s ON ps.supplier_id = s.supplier_id
    WHERE ps.is_primary_supplier = TRUE
),

final AS (
    SELECT
        -- Product identification
        ci.product_id,
        ci.product_name,
        ci.product_category,

        -- Current inventory status
        ci.quantity_on_hand,
        ci.quantity_reserved,
        ci.quantity_available,
        ci.reorder_point,
        ci.reorder_quantity,
        ci.inventory_health,
        ci.is_stockout,
        ci.is_below_reorder_point,

        -- Week-over-week trends
        ci.wow_quantity_change,
        ci.wow_change_percentage,

        -- Sales velocity
        COALESCE(ads.avg_daily_units, 0) AS avg_daily_sales,
        COALESCE(ads.total_sold, 0) AS units_sold_last_30_days,
        COALESCE(ads.days_with_sales, 0) AS days_with_sales_last_30,

        -- Days of supply calculation using custom macro
        {{ calculate_days_of_supply('ci.quantity_available', 'COALESCE(ads.avg_daily_units, 0)') }} AS days_of_supply,

        -- Stockout risk score (0-100, higher = more risk)
        CASE
            WHEN ci.is_stockout THEN 100
            WHEN ci.quantity_available <= 0 THEN 100
            WHEN ads.avg_daily_units IS NULL OR ads.avg_daily_units = 0 THEN 0
            WHEN ci.quantity_available / ads.avg_daily_units <= ps.lead_time_days THEN 90
            WHEN ci.quantity_available / ads.avg_daily_units <= ps.lead_time_days * 1.5 THEN 70
            WHEN ci.is_below_reorder_point THEN 50
            ELSE 10
        END AS stockout_risk_score,

        -- Value at risk (inventory value for items below reorder point)
        CASE
            WHEN ci.is_below_reorder_point OR ci.is_stockout THEN
                ci.quantity_available * COALESCE(ps.unit_cost, 0)
            ELSE 0
        END AS value_at_risk,

        -- Recommended reorder quantity
        CASE
            WHEN ci.is_stockout THEN
                GREATEST(ci.reorder_quantity, ps.minimum_order_quantity)
            WHEN ci.is_below_reorder_point THEN
                GREATEST(
                    ci.reorder_quantity,
                    ps.minimum_order_quantity,
                    ROUND(COALESCE(ads.avg_daily_units, 0) * ps.lead_time_days * 1.5, 0)
                )
            ELSE 0
        END AS recommended_reorder_qty,

        -- Supplier information
        ps.supplier_id,
        ps.supplier_name,
        ps.supplier_tier,
        ps.lead_time_days,
        ps.lead_time_category,
        ps.reliability_score,
        ps.unit_cost AS supplier_unit_cost,
        ps.minimum_order_quantity,

        -- Current inventory value
        ci.quantity_on_hand * COALESCE(ps.unit_cost, 0) AS current_inventory_value,

        -- Snapshot date
        ci.snapshot_date AS last_snapshot_date,

        -- Metadata
        CURRENT_TIMESTAMP AS report_generated_at

    FROM current_inventory ci
    LEFT JOIN avg_daily_sales ads ON ci.product_id = ads.product_id
    LEFT JOIN primary_suppliers ps ON ci.product_id = ps.product_id
)

SELECT * FROM final
ORDER BY stockout_risk_score DESC, days_of_supply ASC NULLS LAST
