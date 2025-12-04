{{ config(materialized='table') }}

-- =============================================================================
-- GOLD LAYER - Inventory Anomalies Report
-- =============================================================================
-- Identifies unusual inventory movements for investigation
-- Anomaly Detection Rules:
-- 1. Large adjustments (>3 standard deviations from mean)
-- 2. Rapid inventory depletion (>50% drop in a week)
-- 3. Negative running balance (data quality issue)
-- 4. Unusual patterns (large weekend transactions)
-- 5. Cost anomalies (unit cost significantly different from average)
-- =============================================================================

WITH transactions AS (
    SELECT * FROM {{ ref('stg_inventory_transactions') }}
),

products AS (
    SELECT
        product_id,
        product_name,
        product_category,
        cost AS product_standard_cost
    FROM {{ ref('stg_products') }}
),

-- Calculate statistics per product
transaction_stats AS (
    SELECT
        product_id,
        AVG(ABS(quantity)) AS avg_transaction_size,
        STDDEV(ABS(quantity)) AS stddev_transaction_size,
        AVG(unit_cost) AS avg_unit_cost,
        STDDEV(unit_cost) AS stddev_unit_cost
    FROM transactions
    GROUP BY product_id
),

-- Get week-over-week inventory changes from snapshots
inventory_changes AS (
    SELECT
        product_id,
        snapshot_date,
        wow_change_percentage,
        inventory_health
    FROM {{ ref('stg_inventory_snapshots') }}
    WHERE wow_change_percentage IS NOT NULL
),

-- Detect anomalies
anomaly_detection AS (
    SELECT
        t.transaction_id,
        t.product_id,
        p.product_name,
        p.product_category,
        t.transaction_type,
        t.quantity,
        t.unit_cost,
        t.transaction_value,
        t.transaction_date,
        t.reference_id,
        t.notes,
        t.is_inbound,
        t.running_inventory_balance,

        -- Statistics for comparison
        s.avg_transaction_size,
        s.stddev_transaction_size,
        s.avg_unit_cost,
        s.stddev_unit_cost,
        p.product_standard_cost,

        -- Calculate z-score for transaction size
        CASE
            WHEN s.stddev_transaction_size > 0 THEN
                ROUND((ABS(t.quantity) - s.avg_transaction_size) / s.stddev_transaction_size, 2)
            ELSE 0
        END AS size_z_score,

        -- Calculate z-score for unit cost
        CASE
            WHEN s.stddev_unit_cost > 0 THEN
                ROUND((t.unit_cost - s.avg_unit_cost) / s.stddev_unit_cost, 2)
            ELSE 0
        END AS cost_z_score,

        -- Day of week (1=Sunday, 7=Saturday)
        DAYOFWEEK(t.transaction_date) AS day_of_week,

        -- Anomaly type flags
        -- 1. Large transaction anomaly (>3 std dev)
        CASE
            WHEN s.stddev_transaction_size > 0
             AND ABS(t.quantity) > s.avg_transaction_size + (3 * s.stddev_transaction_size)
            THEN TRUE
            ELSE FALSE
        END AS is_large_transaction_anomaly,

        -- 2. Negative inventory (data quality issue)
        CASE
            WHEN t.running_inventory_balance < 0 THEN TRUE
            ELSE FALSE
        END AS is_negative_inventory_anomaly,

        -- 3. Weekend activity (unusual for B2B)
        CASE
            WHEN DAYOFWEEK(t.transaction_date) IN (1, 7)  -- Sunday or Saturday
             AND t.transaction_type IN ('restock', 'adjustment')
             AND ABS(t.quantity) > s.avg_transaction_size
            THEN TRUE
            ELSE FALSE
        END AS is_weekend_anomaly,

        -- 4. Cost anomaly (>2 std dev from average)
        CASE
            WHEN s.stddev_unit_cost > 0
             AND ABS(t.unit_cost - s.avg_unit_cost) > (2 * s.stddev_unit_cost)
            THEN TRUE
            ELSE FALSE
        END AS is_cost_anomaly,

        -- 5. Large adjustment without notes
        CASE
            WHEN t.transaction_type = 'adjustment'
             AND ABS(t.quantity) > 10
             AND (t.notes IS NULL OR TRIM(t.notes) = '')
            THEN TRUE
            ELSE FALSE
        END AS is_unexplained_adjustment

    FROM transactions t
    LEFT JOIN products p ON t.product_id = p.product_id
    LEFT JOIN transaction_stats s ON t.product_id = s.product_id
),

-- Add rapid depletion detection from snapshots
rapid_depletion AS (
    SELECT DISTINCT
        product_id,
        snapshot_date,
        'rapid_depletion' AS depletion_type
    FROM inventory_changes
    WHERE wow_change_percentage < -50  -- More than 50% drop
),

final AS (
    SELECT
        ad.*,

        -- Check for rapid depletion
        CASE
            WHEN rd.product_id IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_rapid_depletion_anomaly,

        -- Consolidated anomaly type
        CASE
            WHEN ad.is_negative_inventory_anomaly THEN 'Negative Inventory'
            WHEN ad.is_large_transaction_anomaly THEN 'Large Transaction'
            WHEN ad.is_cost_anomaly THEN 'Cost Anomaly'
            WHEN ad.is_unexplained_adjustment THEN 'Unexplained Adjustment'
            WHEN ad.is_weekend_anomaly THEN 'Weekend Activity'
            WHEN rd.product_id IS NOT NULL THEN 'Rapid Depletion'
            ELSE NULL
        END AS anomaly_type,

        -- Severity score (1-5)
        CASE
            WHEN ad.is_negative_inventory_anomaly THEN 5
            WHEN ad.is_large_transaction_anomaly AND ad.size_z_score > 4 THEN 5
            WHEN ad.is_large_transaction_anomaly THEN 4
            WHEN ad.is_cost_anomaly AND ad.cost_z_score > 3 THEN 4
            WHEN ad.is_unexplained_adjustment THEN 3
            WHEN ad.is_cost_anomaly THEN 3
            WHEN ad.is_weekend_anomaly THEN 2
            WHEN rd.product_id IS NOT NULL THEN 3
            ELSE 1
        END AS severity_score,

        -- Investigation status (placeholder)
        'pending' AS investigation_status,

        -- Metadata
        CURRENT_TIMESTAMP AS report_generated_at

    FROM anomaly_detection ad
    LEFT JOIN rapid_depletion rd
        ON ad.product_id = rd.product_id
        AND ad.transaction_date = rd.snapshot_date
    WHERE
        -- Only include rows with at least one anomaly
        ad.is_large_transaction_anomaly = TRUE
        OR ad.is_negative_inventory_anomaly = TRUE
        OR ad.is_weekend_anomaly = TRUE
        OR ad.is_cost_anomaly = TRUE
        OR ad.is_unexplained_adjustment = TRUE
        OR rd.product_id IS NOT NULL
)

SELECT * FROM final
ORDER BY severity_score DESC, transaction_date DESC
