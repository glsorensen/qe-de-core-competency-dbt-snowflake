{{
    config(
        materialized='table',
        tags=['dimension', 'inventory', 'scd_type2', 'exercise5']
    )
}}

/*
    Inventory History Dimension Table (SCD Type 2)
    ==============================================
    Slowly Changing Dimension Type 2 for tracking inventory level changes over time.
    
    SCD Type 2 Implementation:
    - Each change in inventory status creates a new row
    - valid_from: snapshot_date when status became active
    - valid_to: snapshot_date when status changed (NULL for current)
    - is_current: TRUE for the most recent status of each product
    
    Business Logic:
    - Tracks inventory health status changes over time
    - Maintains full history of inventory transitions
    - Uses LEAD window function to determine validity periods
    
    Grain: One row per product per inventory status change
*/

WITH inventory_snapshots AS (
    SELECT *
    FROM {{ ref('stg_inventory_snapshots') }}
),

-- Identify status changes using LAG to compare with previous snapshot
status_changes AS (
    SELECT
        snapshot_id,
        product_id,
        snapshot_date,
        quantity_on_hand,
        quantity_reserved,
        quantity_available,
        reorder_point,
        reorder_quantity,
        inventory_health,
        is_below_reorder_point,
        is_stockout,
        wow_quantity_change,
        wow_change_percentage,
        
        -- Get previous inventory health to detect changes
        LAG(inventory_health) OVER (
            PARTITION BY product_id 
            ORDER BY snapshot_date
        ) AS previous_inventory_health,
        
        -- Flag when status actually changed
        CASE 
            WHEN LAG(inventory_health) OVER (
                PARTITION BY product_id 
                ORDER BY snapshot_date
            ) != inventory_health 
            OR LAG(inventory_health) OVER (
                PARTITION BY product_id 
                ORDER BY snapshot_date
            ) IS NULL
            THEN TRUE 
            ELSE FALSE 
        END AS is_status_change
    FROM inventory_snapshots
),

-- Keep only records where status changed (or first record)
significant_changes AS (
    SELECT *
    FROM status_changes
    WHERE is_status_change = TRUE
),

-- Calculate validity periods using LEAD to get next change date
scd_type2 AS (
    SELECT
        -- Use ROW_NUMBER to create a unique surrogate key for SCD
        ROW_NUMBER() OVER (ORDER BY product_id, snapshot_date) AS inventory_history_sk,
        
        -- Dimensions
        product_id,
        snapshot_date AS valid_from,
        
        -- Use LEAD to get the next snapshot_date as valid_to
        LEAD(snapshot_date) OVER (
            PARTITION BY product_id 
            ORDER BY snapshot_date
        ) AS valid_to,
        
        -- Current record flag (valid_to IS NULL)
        CASE 
            WHEN LEAD(snapshot_date) OVER (
                PARTITION BY product_id 
                ORDER BY snapshot_date
            ) IS NULL 
            THEN TRUE 
            ELSE FALSE 
        END AS is_current,
        
        -- Inventory metrics at time of change
        quantity_on_hand,
        quantity_reserved,
        quantity_available,
        reorder_point,
        reorder_quantity,
        inventory_health,
        is_below_reorder_point,
        is_stockout,
        wow_quantity_change,
        wow_change_percentage,
        previous_inventory_health,
        
        -- Calculate days in this status
        COALESCE(
            DATEDIFF('day', 
                snapshot_date,
                LEAD(snapshot_date) OVER (
                    PARTITION BY product_id 
                    ORDER BY snapshot_date
                )
            ),
            DATEDIFF('day', snapshot_date, CURRENT_DATE())
        ) AS days_in_status,
        
        -- Audit fields
        CURRENT_TIMESTAMP() AS dbt_updated_at
    FROM significant_changes
),

-- Enrich with product information
final AS (
    SELECT
        scd.inventory_history_sk,
        scd.product_id,
        p.product_name,
        p.category_id,
        scd.valid_from,
        scd.valid_to,
        scd.is_current,
        scd.days_in_status,
        scd.quantity_on_hand,
        scd.quantity_reserved,
        scd.quantity_available,
        scd.reorder_point,
        scd.reorder_quantity,
        scd.inventory_health,
        scd.is_below_reorder_point,
        scd.is_stockout,
        scd.wow_quantity_change,
        scd.wow_change_percentage,
        scd.previous_inventory_health,
        
        -- Status transition tracking
        CASE 
            WHEN scd.previous_inventory_health IS NOT NULL 
            THEN CONCAT(scd.previous_inventory_health, ' -> ', scd.inventory_health)
            ELSE CONCAT('Initial -> ', scd.inventory_health)
        END AS status_transition,
        
        scd.dbt_updated_at
    FROM scd_type2 scd
    LEFT JOIN {{ ref('stg_products') }} p
        ON scd.product_id = p.product_id
)

SELECT * FROM final
