{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge',
        merge_update_columns=['transaction_value', 'is_inbound', 'is_outbound', 'extracted_order_id', 'notes'],
        tags=['fact', 'inventory', 'incremental', 'exercise5']
    )
}}

/*
    Inventory Movements Fact Table (Incremental)
    ============================================
    Transaction-level fact table for all inventory movements.
    
    Incremental Strategy:
    - Uses merge to handle updates to existing transactions
    - unique_key: transaction_id ensures no duplicates
    - Only processes new/changed transactions after initial load
    
    Business Logic:
    - All inventory inbound/outbound movements
    - Links to products and suppliers where applicable
    - Tracks running inventory balance per product
    - Enriched with product and supplier context
    
    Grain: One row per inventory transaction
*/

WITH inventory_transactions AS (
    SELECT *
    FROM {{ ref('stg_inventory_transactions') }}
    {% if is_incremental() %}
    -- Only process new transactions since last run
    WHERE transaction_date > (SELECT MAX(transaction_date) FROM {{ this }})
    {% endif %}
),

products AS (
    SELECT
        product_id,
        product_name,
        category_id,
        price AS product_retail_price
    FROM {{ ref('stg_products') }}
),

-- Get primary supplier for each product
primary_suppliers AS (
    SELECT
        product_id,
        supplier_id,
        supplier_name,
        supplier_country,
        unit_cost AS primary_supplier_cost
    FROM {{ ref('stg_product_suppliers') }}
    WHERE is_primary_supplier = TRUE
),

final AS (
    SELECT
        -- Transaction identifiers
        it.transaction_id,
        it.product_id,
        it.transaction_date,
        
        -- Transaction details
        it.transaction_type,
        it.quantity,
        it.unit_cost,
        it.transaction_value,
        it.reference_id,
        it.extracted_order_id,
        it.notes,
        
        -- Transaction classification
        it.is_inbound,
        it.is_outbound,
        it.transaction_sequence,
        it.running_inventory_balance,
        
        -- Product context (enriched)
        p.product_name,
        p.category_id,
        p.product_retail_price,
        
        -- Supplier context (primary supplier)
        ps.supplier_id AS primary_supplier_id,
        ps.supplier_name AS primary_supplier_name,
        ps.supplier_country AS primary_supplier_country,
        ps.primary_supplier_cost,
        
        -- Cost variance analysis
        CASE 
            WHEN it.unit_cost IS NOT NULL AND ps.primary_supplier_cost IS NOT NULL
            THEN ROUND(it.unit_cost - ps.primary_supplier_cost, 2)
            ELSE NULL
        END AS cost_variance_from_primary,
        
        -- Value metrics
        CASE 
            WHEN p.product_retail_price IS NOT NULL AND it.quantity < 0
            THEN ROUND(ABS(it.quantity) * p.product_retail_price, 2)
            ELSE NULL
        END AS potential_revenue_value,
        
        -- Time-based attributes
        DATE_TRUNC('month', it.transaction_date) AS transaction_month,
        DATE_TRUNC('week', it.transaction_date) AS transaction_week,
        DAYNAME(it.transaction_date) AS transaction_day_of_week,
        
        -- Audit fields
        CURRENT_TIMESTAMP() AS dbt_updated_at
    FROM inventory_transactions it
    LEFT JOIN products p
        ON it.product_id = p.product_id
    LEFT JOIN primary_suppliers ps
        ON it.product_id = ps.product_id
)

SELECT * FROM final
