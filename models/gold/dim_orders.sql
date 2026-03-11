{{ config(
    materialized='table',
    tags=['gold', 'orders']
) }}

-- ============================================================================
-- GOLD LAYER - ORDERS DIMENSION
-- ============================================================================
-- Business-ready orders dimension with denormalized customer and product info
--
-- Key Features:
-- - One row per order
-- - Denormalized customer and product details
-- - Order status categorization
-- - Date processing for analysis
-- ============================================================================

WITH orders as (
    SELECT
        o.order_id,
        o.customer_id,
        o.product_id,
        o.quantity,
        o.order_date,
        o.total_amount,
        o.status,
        c.first_name,
        c.last_name,
        c.email,
        c.city,
        c.state,
        p.product_name,
        p.category,
        p.price as product_price,
        CASE
            WHEN o.status = 'completed' THEN 'completed'
            WHEN o.status = 'pending' THEN 'pending'
            WHEN o.status = 'processing' THEN 'processing'
            ELSE 'other'
        END as order_status_category,
        YEAR(o.order_date) as order_year,
        MONTH(o.order_date) as order_month,
        QUARTER(o.order_date) as order_quarter,
        DATE_TRUNC('week', o.order_date) as order_week
    FROM {{ ref('stg_orders') }} o
    LEFT JOIN {{ ref('dim_customers') }} c ON o.customer_id = c.customer_id
    LEFT JOIN {{ ref('dim_products') }} p ON o.product_id = p.product_id
)

SELECT * FROM orders
