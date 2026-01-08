{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'inventory_snapshots') }}
),

transformed AS (
    SELECT
        -- Primary Key
        snapshot_id,
        
        -- Foreign Keys
        product_id,
        
        -- Snapshot details
        snapshot_date::DATE AS snapshot_date,
        quantity_on_hand,
        quantity_reserved,
        quantity_available,
        reorder_point,
        reorder_quantity,
        
        -- Calculated: Is below reorder point?
        CASE
            WHEN quantity_available <= reorder_point THEN TRUE
            ELSE FALSE
        END AS is_below_reorder_point,
        
        -- Calculated: Is stockout?
        CASE
            WHEN quantity_available = 0 THEN TRUE
            ELSE FALSE
        END AS is_stockout,
        
        -- Calculated: Inventory health status
        CASE
            WHEN quantity_available = 0 THEN 'critical'
            WHEN quantity_available <= reorder_point THEN 'low'
            WHEN quantity_available > (reorder_point * 3) THEN 'overstocked'
            ELSE 'healthy'
        END AS inventory_health,
        
        -- Window Function: Previous week's quantity
        LAG(quantity_on_hand, 1) OVER (
            PARTITION BY product_id
            ORDER BY snapshot_date
        ) AS previous_quantity_on_hand,
        
        -- Calculated: Week-over-week quantity change
        quantity_on_hand - LAG(quantity_on_hand, 1) OVER (
            PARTITION BY product_id
            ORDER BY snapshot_date
        ) AS wow_quantity_change,
        
        -- Calculated: Week-over-week percentage change
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
        
        -- Window Function: Days since last snapshot for this product
        DATEDIFF('day', 
            LAG(snapshot_date, 1) OVER (
                PARTITION BY product_id
                ORDER BY snapshot_date
            ),
            snapshot_date
        ) AS days_since_last_snapshot,
        
        -- Calculated: Snapshot age (days from snapshot to today)
        DATEDIFF('day', snapshot_date, CURRENT_DATE) AS snapshot_age_days,
        
        -- Window Function: Is this the most recent snapshot for the product?
        CASE
            WHEN ROW_NUMBER() OVER (
                PARTITION BY product_id
                ORDER BY snapshot_date DESC
            ) = 1 THEN TRUE
            ELSE FALSE
        END AS is_current_snapshot,
        
        -- Metadata
        CURRENT_TIMESTAMP AS _loaded_at

    FROM source
)

SELECT * FROM transformed
