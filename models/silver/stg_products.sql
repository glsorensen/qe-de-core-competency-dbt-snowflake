{{ config(
    materialized='view',
    tags=['silver', 'staging']
) }}

-- ============================================================================
-- SILVER LAYER - STAGING PRODUCTS
-- ============================================================================
-- Transforms raw product catalog into clean, standardized format
--
-- Transformations applied:
-- - Name and brand standardization
-- - Category normalization
-- - Profit margin calculations
-- - Price categorization
-- ============================================================================

WITH source_data AS (
    SELECT
        product_id,
        product_name,
        product_category,
        product_subcategory,
        brand,
        price,
        cost,
        sku,
        category_id,
        created_at,
        updated_at,
        is_active
    FROM {{ source('raw', 'products') }}
)

SELECT
    product_id,
    category_id,

    -- Name standardization
    TRIM(INITCAP(product_name)) AS product_name,

    -- Category standardization
    LOWER(REPLACE(product_category, ' ', '_')) AS product_category,
    LOWER(REPLACE(product_subcategory, ' ', '_')) AS product_subcategory,
    TRIM(INITCAP(brand)) AS brand,
    sku,

    -- Financial fields
    ROUND(price, 2) AS price,
    ROUND(cost, 2) AS cost,
    
    -- Price calculations
    CASE 
        WHEN price > 0 THEN ROUND((price - cost) / price * 100, 2)
        ELSE NULL
    END AS profit_margin_pct,
    
    CASE 
        WHEN price > 0 THEN price - cost
        ELSE NULL
    END AS profit_amount,
    
    -- Price categorization
    CASE 
        WHEN price < 50 THEN 'budget'
        WHEN price <= 150 THEN 'mid_range'
        ELSE 'premium'
    END AS price_category,
    
    -- Date fields
    created_at,
    updated_at,
    
    -- Status
    is_active,
    
    -- Derived fields
    DATEDIFF('day', created_at, CURRENT_DATE()) AS days_since_created,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM source_data
WHERE product_id IS NOT NULL
  AND price IS NOT NULL
  AND price > 0
  AND cost IS NOT NULL
  AND is_active = TRUE