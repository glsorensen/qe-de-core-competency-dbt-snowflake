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
        order_date,
        order_status,
        total_amount,
        shipping_cost,
        tax_amount,
        discount_amount,
        payment_method,
        shipping_address,
        created_at
    FROM {{ source('raw', 'orders') }}
),

cleaned AS (
    SELECT
        -- Primary key
        order_id,

        -- Foreign keys
        customer_id,

        -- Order details
        order_date::DATE AS order_date,
        LOWER(TRIM(order_status)) AS order_status,

        -- Financial fields - ensure proper decimal handling
        ROUND(COALESCE(total_amount, 0), 2) AS total_amount,
        ROUND(COALESCE(shipping_cost, 0), 2) AS shipping_cost,
        ROUND(COALESCE(tax_amount, 0), 2) AS tax_amount,
        ROUND(COALESCE(discount_amount, 0), 2) AS discount_amount,

        -- Calculate subtotal (total - shipping - tax + discount)
        ROUND(
            COALESCE(total_amount, 0)
            - COALESCE(shipping_cost, 0)
            - COALESCE(tax_amount, 0)
            + COALESCE(discount_amount, 0),
            2
        ) AS subtotal_amount,

        -- Payment information
        LOWER(TRIM(payment_method)) AS payment_method,
        TRIM(shipping_address) AS shipping_address,

        -- Timestamps
        created_at,

        -- Derived fields
        DATE_PART('year', order_date) AS order_year,
        DATE_PART('month', order_date) AS order_month,
        DATE_PART('quarter', order_date) AS order_quarter,
        TO_CHAR(order_date, 'YYYY-MM') AS order_year_month,
        DAYNAME(order_date) AS order_day_of_week,

        -- Business logic flags
        CASE
            WHEN order_status IN ('delivered', 'completed') THEN TRUE
            ELSE FALSE
        END AS is_completed,

        CASE
            WHEN order_status = 'cancelled' THEN TRUE
            ELSE FALSE
        END AS is_cancelled,

        CASE
            WHEN discount_amount > 0 THEN TRUE
            ELSE FALSE
        END AS has_discount,

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
    AND order_date IS NOT NULL
    AND created_at IS NOT NULL
    AND total_amount >= 0  -- No negative totals
    AND order_status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled', 'returned')
