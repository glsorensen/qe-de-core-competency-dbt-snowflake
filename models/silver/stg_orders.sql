{{ config(
    materialized='view',
    tags=['silver', 'staging']
) }}

-- ============================================================================
-- SILVER LAYER - STAGING ORDERS
-- ============================================================================
-- Transforms raw orders data into clean, standardized format
--
-- Transformations applied:
-- - Status standardization and validation
-- - Amount calculations and validation
-- - Date/time standardization
-- - Data quality filters
-- ============================================================================

WITH source_data AS (
    SELECT
        order_id,
        customer_id,
        product_id,
        quantity,
        order_date,
        total_amount,
        status
    FROM {{ source('raw', 'orders') }}
),

cleaned AS (
    SELECT
        -- Primary key
        order_id,

        -- Foreign keys
        customer_id,
        product_id,

        -- Order details
        order_date::DATE AS order_date,
        LOWER(TRIM(status)) AS status,
        
        -- Quantity
        quantity,

        -- Financial fields
        ROUND(COALESCE(total_amount, 0), 2) AS total_amount,

        -- Timestamps
        CURRENT_TIMESTAMP() AS created_at,

        -- Derived date fields
        YEAR(order_date) AS order_year,
        MONTH(order_date) AS order_month,
        QUARTER(order_date) AS order_quarter,
        TO_CHAR(order_date, 'YYYY-MM') AS order_year_month,
        DAYNAME(order_date) AS order_day_of_week,

        -- Metadata
        CURRENT_TIMESTAMP() AS dbt_updated_at

    FROM source_data
)

SELECT *
FROM cleaned
WHERE
    -- Data quality filters
    order_id IS NOT NULL
    AND customer_id IS NOT NULL
    AND product_id IS NOT NULL
    AND order_date IS NOT NULL
    AND total_amount >= 0

    order_id IS NOT NULL
    AND customer_id IS NOT NULL
    AND order_date IS NOT NULL
    AND created_at IS NOT NULL
    AND total_amount >= 0  -- No negative totals
    AND order_status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled', 'returned')
