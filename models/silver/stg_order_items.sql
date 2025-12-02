{{ config(
    materialized='view',
    tags=['silver', 'staging']
) }}

-- ============================================================================
-- SILVER LAYER - STAGING ORDER ITEMS
-- ============================================================================
-- Transforms raw order line items into clean, standardized format
--
-- Transformations applied:
-- - Amount calculations and validation
-- - Quantity validation
-- - Derived metrics (discounts, margins)
-- - Data quality filters
-- ============================================================================

WITH source_data AS (
    SELECT
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        line_total,
        discount_applied,
        created_at
    FROM {{ source('raw', 'order_items') }}
),

cleaned AS (
    SELECT
        -- Primary key
        order_item_id,

        -- Foreign keys
        order_id,
        product_id,

        -- Quantity and pricing
        quantity,
        ROUND(COALESCE(unit_price, 0), 2) AS unit_price,
        ROUND(COALESCE(line_total, 0), 2) AS line_total,
        ROUND(COALESCE(discount_applied, 0), 2) AS discount_applied,

        -- Calculate line total if not provided or incorrect
        ROUND(quantity * COALESCE(unit_price, 0), 2) AS calculated_line_total,

        -- Calculate line total after discount
        ROUND(
            COALESCE(line_total, quantity * unit_price)
            - COALESCE(discount_applied, 0),
            2
        ) AS line_total_after_discount,

        -- Discount percentage
        CASE
            WHEN COALESCE(line_total, quantity * unit_price) > 0 THEN
                ROUND(
                    (COALESCE(discount_applied, 0) / NULLIF(COALESCE(line_total, quantity * unit_price), 0)) * 100,
                    2
                )
            ELSE 0
        END AS discount_percentage,

        -- Flags
        CASE
            WHEN discount_applied > 0 THEN TRUE
            ELSE FALSE
        END AS has_discount,

        -- Timestamps
        created_at,

        -- Metadata
        CURRENT_TIMESTAMP() AS dbt_updated_at

    FROM source_data
)

SELECT *
FROM cleaned
WHERE
    -- Data quality filters
    order_item_id IS NOT NULL
    AND order_id IS NOT NULL
    AND product_id IS NOT NULL
    AND quantity > 0  -- Must have at least 1 item
    AND unit_price >= 0  -- No negative prices
    AND line_total >= 0  -- No negative line totals
