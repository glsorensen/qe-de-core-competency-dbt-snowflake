{{ config(materialized='view') }}

-- =============================================================================
-- SILVER LAYER - Staging Inventory Transactions
-- =============================================================================
-- Clean and enrich inventory transaction data with running totals
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ source('raw', 'inventory_transactions') }}
),

cleaned AS (
    SELECT
        -- Primary key
        transaction_id,

        -- Foreign key
        product_id,

        -- Standardized transaction type
        LOWER(TRIM(transaction_type)) AS transaction_type,

        -- Quantity (ensure sign matches transaction type)
        CASE
            WHEN LOWER(TRIM(transaction_type)) IN ('sale', 'damaged') AND quantity > 0 THEN -quantity
            WHEN LOWER(TRIM(transaction_type)) IN ('restock', 'return') AND quantity < 0 THEN -quantity
            ELSE quantity
        END AS quantity,

        -- Original quantity for reference
        quantity AS original_quantity,

        -- Cost
        unit_cost,

        -- Derived: Transaction value
        ABS(quantity) * unit_cost AS transaction_value,

        -- Transaction date
        transaction_date,

        -- Reference parsing
        reference_id,
        CASE
            WHEN reference_id LIKE 'ORD-%' THEN REPLACE(reference_id, 'ORD-', '')
            ELSE NULL
        END AS order_number,
        CASE
            WHEN reference_id LIKE 'PO-%' THEN REPLACE(reference_id, 'PO-', '')
            ELSE NULL
        END AS po_number,

        -- Notes
        TRIM(notes) AS notes,

        -- Derived: Is this an inbound transaction?
        CASE
            WHEN LOWER(TRIM(transaction_type)) IN ('restock', 'return') THEN TRUE
            ELSE FALSE
        END AS is_inbound

    FROM source
),

with_running_totals AS (
    SELECT
        *,
        -- Running inventory balance per product
        SUM(quantity) OVER (
            PARTITION BY product_id
            ORDER BY transaction_date, transaction_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_inventory_balance

    FROM cleaned
)

SELECT * FROM with_running_totals
