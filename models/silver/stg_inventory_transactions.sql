{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'inventory_transactions') }}
),

transformed AS (
    SELECT
        -- Primary Key
        transaction_id,
        
        -- Foreign Keys
        product_id,
        
        -- Transaction details
        LOWER(TRIM(transaction_type)) AS transaction_type,
        quantity,
        unit_cost,
        transaction_date::DATE AS transaction_date,
        TRIM(reference_id) AS reference_id,
        TRIM(notes) AS notes,
        
        -- Calculated: Transaction value (absolute value of quantity * cost)
        ROUND(ABS(quantity) * unit_cost, 2) AS transaction_value,
        
        -- Calculated: Is this an inbound transaction (adds inventory)?
        CASE
            WHEN LOWER(TRIM(transaction_type)) IN ('restock', 'return') THEN TRUE
            ELSE FALSE
        END AS is_inbound,
        
        -- Calculated: Is this an outbound transaction (removes inventory)?
        CASE
            WHEN LOWER(TRIM(transaction_type)) IN ('sale', 'damaged') THEN TRUE
            ELSE FALSE
        END AS is_outbound,
        
        -- Calculated: Extract order number from reference_id if it's an order
        CASE
            WHEN reference_id LIKE 'ORD-%' THEN TRY_CAST(REPLACE(reference_id, 'ORD-', '') AS INTEGER)
            ELSE NULL
        END AS extracted_order_id,
        
        -- Window Function: Running inventory balance per product
        SUM(quantity) OVER (
            PARTITION BY product_id
            ORDER BY transaction_date, transaction_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_inventory_balance,
        
        -- Window Function: Transaction sequence number per product
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY transaction_date, transaction_id
        ) AS transaction_sequence,
        
        -- Metadata
        CURRENT_TIMESTAMP AS _loaded_at

    FROM source
)

SELECT * FROM transformed
