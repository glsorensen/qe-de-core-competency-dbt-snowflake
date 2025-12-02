{{ config(
    materialized='table',
    tags=['gold', 'fact']
) }}

-- ============================================================================
-- GOLD LAYER - ORDER ITEMS FACT TABLE
-- ============================================================================
-- Business-ready fact table for line-item level analysis
--
-- Features:
-- - Product and order context
-- - Profit margin calculations
-- - Customer denormalization
-- - Time dimensions
-- ============================================================================

WITH order_items AS (
    SELECT *
    FROM {{ ref('stg_order_items') }}
),

orders AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        order_status,
        order_year,
        order_month,
        order_quarter,
        is_completed,
        is_cancelled
    FROM {{ ref('stg_orders') }}
),

products AS (
    SELECT
        product_id,
        product_name,
        product_category,
        product_subcategory,
        brand,
        price AS current_price,
        cost AS current_cost,
        profit_margin_pct AS current_profit_margin_pct
    FROM {{ ref('stg_products') }}
),

customers AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        acquisition_channel
    FROM {{ ref('stg_customers') }}
)

SELECT
    -- Line item identifier
    oi.order_item_id,

    -- Foreign keys
    oi.order_id,
    oi.product_id,
    o.customer_id,

    -- Denormalized dimensions for easy filtering
    p.product_name,
    p.product_category,
    p.product_subcategory,
    p.brand,
    c.first_name AS customer_first_name,
    c.last_name AS customer_last_name,
    c.acquisition_channel AS customer_acquisition_channel,

    -- Order context
    o.order_date,
    o.order_status,
    o.order_year,
    o.order_month,
    o.order_quarter,

    -- Line item metrics
    oi.quantity,
    oi.unit_price,
    oi.line_total,
    oi.discount_applied,
    oi.line_total_after_discount,
    oi.discount_percentage,

    -- Profit calculations using current cost
    -- Note: This uses current product cost, not historical cost at time of sale
    ROUND(oi.quantity * p.current_cost, 2) AS estimated_cost,
    ROUND(oi.line_total_after_discount - (oi.quantity * p.current_cost), 2) AS estimated_profit,

    -- Calculate profit margin for this line item
    CASE
        WHEN oi.line_total_after_discount > 0 THEN
            ROUND(
                ((oi.line_total_after_discount - (oi.quantity * p.current_cost)) / oi.line_total_after_discount) * 100,
                2
            )
        ELSE 0
    END AS estimated_profit_margin_pct,

    -- Price comparison to current price
    ROUND(oi.unit_price - p.current_price, 2) AS price_difference_from_current,

    CASE
        WHEN oi.unit_price > p.current_price THEN 'higher_than_current'
        WHEN oi.unit_price < p.current_price THEN 'lower_than_current'
        ELSE 'same_as_current'
    END AS price_vs_current,

    -- Business flags
    o.is_completed,
    o.is_cancelled,
    oi.has_discount,

    CASE
        WHEN oi.quantity >= 3 THEN TRUE
        ELSE FALSE
    END AS is_bulk_purchase,

    CASE
        WHEN oi.line_total_after_discount >= 100 THEN TRUE
        ELSE FALSE
    END AS is_high_value_line,

    -- Metadata
    oi.created_at AS line_item_created_at,
    oi.dbt_updated_at

FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.order_id
INNER JOIN products p ON oi.product_id = p.product_id
LEFT JOIN customers c ON o.customer_id = c.customer_id
