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
-- - Name and category standardization
-- - Price validation
-- - Data quality filters
-- ============================================================================

WITH source_data AS (
    SELECT
        product_id,
        product_name,
        category,
        price,
        stock_quantity,
        description
    FROM {{ source('raw', 'products') }}
)

SELECT
    product_id,

    -- Name standardization
    TRIM(INITCAP(product_name)) AS product_name,

    -- Category standardization
    LOWER(REPLACE(category, ' ', '_')) AS category,

    -- Financial fields
    ROUND(price, 2) AS price,
    
    -- Stock information
    stock_quantity,
    
    -- Description
    description,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM source_data
WHERE product_id IS NOT NULL
  AND price IS NOT NULL
  AND price > 0
