{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge'
) }}

-- =============================================================================
-- GOLD LAYER - Inventory Movements Fact Table (Incremental)
-- =============================================================================
-- Transaction-level fact table for inventory movements
-- Uses incremental materialization for efficient processing
-- =============================================================================

WITH transactions AS (
    SELECT * FROM {{ ref('stg_inventory_transactions') }}
    {% if is_incremental() %}
        WHERE transaction_date > (SELECT MAX(transaction_date) FROM {{ this }})
    {% endif %}
),

products AS (
    SELECT
        product_id,
        product_name,
        product_category,
        price AS retail_price
    FROM {{ ref('stg_products') }}
),

-- Get primary supplier for each product
primary_suppliers AS (
    SELECT
        ps.product_id,
        ps.supplier_id,
        s.supplier_name,
        s.supplier_tier
    FROM {{ ref('stg_product_suppliers') }} ps
    INNER JOIN {{ ref('dim_suppliers') }} s ON ps.supplier_id = s.supplier_id
    WHERE ps.is_primary_supplier = TRUE
),

final AS (
    SELECT
        -- Transaction key
        t.transaction_id,

        -- Foreign keys
        t.product_id,
        ps.supplier_id,

        -- Product dimensions (for convenience)
        p.product_name,
        p.product_category,

        -- Supplier dimensions (for restocks)
        ps.supplier_name,
        ps.supplier_tier,

        -- Transaction details
        t.transaction_type,
        t.quantity,
        t.unit_cost,
        t.transaction_value,
        t.transaction_date,

        -- Reference information
        t.reference_id,
        t.order_number,
        t.po_number,
        t.notes,

        -- Flags
        t.is_inbound,

        -- Running totals
        t.running_inventory_balance,

        -- Inventory value change (signed)
        t.quantity * t.unit_cost AS inventory_value_change,

        -- Retail value impact (for sales)
        CASE
            WHEN t.transaction_type = 'sale' THEN ABS(t.quantity) * p.retail_price
            ELSE NULL
        END AS retail_value_impact,

        -- Transaction ranking per product (most recent = 1)
        ROW_NUMBER() OVER (
            PARTITION BY t.product_id
            ORDER BY t.transaction_date DESC, t.transaction_id DESC
        ) AS transaction_recency_rank,

        -- Metadata
        CURRENT_TIMESTAMP AS dbt_updated_at

    FROM transactions t
    LEFT JOIN products p ON t.product_id = p.product_id
    LEFT JOIN primary_suppliers ps ON t.product_id = ps.product_id
)

SELECT * FROM final
