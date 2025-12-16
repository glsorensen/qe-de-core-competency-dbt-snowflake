{{ config(materialized='table') }}

-- =============================================================================
-- GOLD LAYER - Inventory History Dimension (SCD Type 2)
-- =============================================================================
-- Tracks historical inventory states with validity periods
-- Enables point-in-time inventory lookups
-- =============================================================================

WITH snapshots AS (
    SELECT * FROM {{ ref('stg_inventory_snapshots') }}
),

products AS (
    SELECT
        product_id,
        product_name,
        product_category
    FROM {{ ref('stg_products') }}
),

with_validity AS (
    SELECT
        -- Surrogate key combining product and snapshot date
        {{ dbt_utils.generate_surrogate_key(['s.product_id', 's.snapshot_date']) }} AS inventory_sk,

        -- Natural keys
        s.product_id,
        s.snapshot_id,

        -- Product info for convenience
        p.product_name,
        p.product_category,

        -- Snapshot date
        s.snapshot_date,

        -- Inventory quantities
        s.quantity_on_hand,
        s.quantity_reserved,
        s.quantity_available,

        -- Reorder thresholds
        s.reorder_point,
        s.reorder_quantity,

        -- Health indicators
        s.is_below_reorder_point,
        s.is_stockout,
        s.inventory_health,

        -- Week-over-week changes
        s.wow_quantity_change,
        s.wow_change_percentage,

        -- SCD Type 2 validity: valid_from is the snapshot date
        s.snapshot_date AS valid_from,

        -- valid_to is the day before the next snapshot, or far future if current
        COALESCE(
            DATEADD(day, -1, LEAD(s.snapshot_date) OVER (
                PARTITION BY s.product_id
                ORDER BY s.snapshot_date
            )),
            '9999-12-31'::DATE
        ) AS valid_to,

        -- Is this the current (most recent) record?
        CASE
            WHEN LEAD(s.snapshot_date) OVER (
                PARTITION BY s.product_id
                ORDER BY s.snapshot_date
            ) IS NULL THEN TRUE
            ELSE FALSE
        END AS is_current,

        -- Row number for the product (1 = oldest)
        ROW_NUMBER() OVER (
            PARTITION BY s.product_id
            ORDER BY s.snapshot_date
        ) AS snapshot_sequence,

        -- Metadata
        CURRENT_TIMESTAMP AS dbt_updated_at

    FROM snapshots s
    LEFT JOIN products p ON s.product_id = p.product_id
)

SELECT * FROM with_validity
