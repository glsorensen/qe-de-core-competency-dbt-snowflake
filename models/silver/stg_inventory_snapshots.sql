{{ config(materialized='view') }}

-- =============================================================================
-- SILVER LAYER - Staging Inventory Snapshots
-- =============================================================================
-- Clean inventory snapshots with health status and week-over-week changes
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ source('raw', 'inventory_snapshots') }}
),

cleaned AS (
    SELECT
        -- Primary key
        snapshot_id,

        -- Foreign key
        product_id,

        -- Snapshot date
        snapshot_date,

        -- Inventory quantities
        quantity_on_hand,
        quantity_reserved,
        quantity_available,

        -- Reorder thresholds
        reorder_point,
        reorder_quantity,

        -- Derived: Is below reorder point?
        CASE
            WHEN quantity_available <= reorder_point THEN TRUE
            ELSE FALSE
        END AS is_below_reorder_point,

        -- Derived: Is stockout?
        CASE
            WHEN quantity_available = 0 THEN TRUE
            ELSE FALSE
        END AS is_stockout,

        -- Derived: Inventory health status
        CASE
            WHEN quantity_available = 0 THEN 'critical'
            WHEN quantity_available <= reorder_point THEN 'low'
            WHEN quantity_available > reorder_point * 3 THEN 'overstocked'
            ELSE 'healthy'
        END AS inventory_health

    FROM source
),

with_wow_changes AS (
    SELECT
        c.*,

        -- Week-over-week quantity change
        quantity_on_hand - LAG(quantity_on_hand, 1) OVER (
            PARTITION BY product_id
            ORDER BY snapshot_date
        ) AS wow_quantity_change,

        -- Week-over-week percentage change
        ROUND(
            (quantity_on_hand - LAG(quantity_on_hand, 1) OVER (
                PARTITION BY product_id
                ORDER BY snapshot_date
            )) * 100.0 / NULLIF(LAG(quantity_on_hand, 1) OVER (
                PARTITION BY product_id
                ORDER BY snapshot_date
            ), 0),
            2
        ) AS wow_change_percentage,

        -- Previous snapshot date for reference
        LAG(snapshot_date, 1) OVER (
            PARTITION BY product_id
            ORDER BY snapshot_date
        ) AS previous_snapshot_date

    FROM cleaned c
)

SELECT * FROM with_wow_changes
