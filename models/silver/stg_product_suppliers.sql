{{ config(materialized='view') }}

-- =============================================================================
-- SILVER LAYER - Staging Product Suppliers
-- =============================================================================
-- Product-supplier mappings with margin calculations
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ source('raw', 'product_suppliers') }}
),

products AS (
    SELECT 
        product_id,
        price
    FROM {{ ref('stg_products') }}
),

cleaned AS (
    SELECT
        -- Primary key
        ps.product_supplier_id,

        -- Foreign keys
        ps.product_id,
        ps.supplier_id,

        -- Pricing
        ps.unit_cost,
        p.price AS product_retail_price,

        -- Derived: Cost variance from retail
        p.price - ps.unit_cost AS cost_variance_from_retail,

        -- Derived: Margin percentage
        CASE
            WHEN p.price IS NULL OR p.price = 0 THEN NULL
            ELSE ROUND(((p.price - ps.unit_cost) / p.price) * 100, 2)
        END AS margin_percentage,

        -- Order constraints
        ps.minimum_order_quantity,

        -- Supplier status
        COALESCE(ps.is_primary_supplier, FALSE) AS is_primary_supplier,

        -- Dates
        ps.effective_date,

        -- Data quality flag: unit cost exceeds retail price
        CASE
            WHEN ps.unit_cost > p.price THEN TRUE
            ELSE FALSE
        END AS has_cost_exceeds_price_issue

    FROM source ps
    LEFT JOIN products p ON ps.product_id = p.product_id
)

SELECT * FROM cleaned
