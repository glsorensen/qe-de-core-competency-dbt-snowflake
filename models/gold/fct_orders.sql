{{ config(
    materialized='table',
    tags=['gold', 'fact']
) }}

-- ============================================================================
-- GOLD LAYER - ORDERS FACT TABLE
-- ============================================================================
-- Business-ready fact table for order-level analysis
--
-- Features:
-- - Denormalized customer information for easy filtering
-- - Complete order financial breakdown
-- - Time dimensions for analysis
-- - Business flags and metrics
-- ============================================================================

WITH orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        acquisition_channel,
        customer_age,
        days_since_signup
    FROM {{ ref('stg_customers') }}
),

order_items_agg AS (
    SELECT
        order_id,
        COUNT(DISTINCT product_id) AS unique_products,
        SUM(quantity) AS total_items,
        SUM(line_total_after_discount) AS calculated_subtotal
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
)

SELECT
    -- Order identifiers
    o.order_id,

    -- Customer information (denormalized for easy filtering)
    o.customer_id,
    c.first_name AS customer_first_name,
    c.last_name AS customer_last_name,
    c.acquisition_channel AS customer_acquisition_channel,

    -- Order details
    o.order_date,
    o.order_status,
    o.payment_method,

    -- Time dimensions
    o.order_year,
    o.order_month,
    o.order_quarter,
    o.order_year_month,
    o.order_day_of_week,

    -- Financial metrics
    o.subtotal_amount,
    o.shipping_cost,
    o.tax_amount,
    o.discount_amount,
    o.total_amount,

    -- Calculate discount percentage
    CASE
        WHEN o.total_amount > 0 THEN
            ROUND((o.discount_amount / o.total_amount) * 100, 2)
        ELSE 0
    END AS discount_percentage,

    -- Calculate profit contribution (without product costs for now)
    o.total_amount - o.discount_amount AS net_revenue,

    -- Order item metrics
    COALESCE(oi.unique_products, 0) AS unique_products_count,
    COALESCE(oi.total_items, 0) AS total_items_quantity,

    -- Average item value
    CASE
        WHEN oi.total_items > 0 THEN
            ROUND(o.subtotal_amount / oi.total_items, 2)
        ELSE 0
    END AS avg_item_value,

    -- Business flags
    o.is_completed,
    o.is_cancelled,
    o.has_discount,

    CASE
        WHEN o.total_amount >= 100 THEN TRUE
        ELSE FALSE
    END AS is_large_order,

    CASE
        WHEN oi.unique_products >= 5 THEN TRUE
        ELSE FALSE
    END AS is_multi_product_order,

    -- Customer context at time of order
    c.customer_age AS customer_age_at_order,
    c.days_since_signup AS customer_tenure_at_order,

    -- Metadata
    o.shipping_address,
    o.created_at AS order_created_at,
    o.dbt_updated_at

FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN order_items_agg oi ON o.order_id = oi.order_id
