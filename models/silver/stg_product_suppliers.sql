{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'product_suppliers') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

suppliers AS (
    SELECT * FROM {{ ref('stg_suppliers') }}
),

transformed AS (
    SELECT
        -- Primary Key
        ps.product_supplier_id,
        
        -- Foreign Keys
        ps.product_id,
        ps.supplier_id,
        
        -- Relationship attributes
        ps.unit_cost,
        ps.minimum_order_quantity,
        ps.is_primary_supplier,
        ps.effective_date::DATE AS effective_date,
        
        -- Product details (for validation and analysis)
        p.product_name,
        p.price AS product_retail_price,
        
        -- Supplier details
        s.supplier_name,
        s.country AS supplier_country,
        s.lead_time_days,
        
        -- Calculated: Cost variance from retail
        (p.price - ps.unit_cost) AS cost_variance_from_retail,
        
        -- Calculated: Margin percentage
        ROUND(((p.price - ps.unit_cost) / NULLIF(p.price, 0)) * 100, 2) AS margin_percentage,
        
        -- Data Quality Flag: Unit cost should not exceed retail price
        CASE
            WHEN ps.unit_cost > p.price THEN TRUE
            ELSE FALSE
        END AS cost_exceeds_price_flag,
        
        -- Metadata
        CURRENT_TIMESTAMP AS _loaded_at

    FROM source ps
    LEFT JOIN products p ON ps.product_id = p.product_id
    LEFT JOIN suppliers s ON ps.supplier_id = s.supplier_id
)

SELECT * FROM transformed
