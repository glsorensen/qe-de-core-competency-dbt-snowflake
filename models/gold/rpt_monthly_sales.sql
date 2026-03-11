{{ config(
    materialized='table',
    tags=['gold', 'report']
) }}

-- ============================================================================
-- GOLD LAYER - MONTHLY SALES REPORT
-- ============================================================================
-- Executive-ready monthly sales performance report
--
-- Features:
-- - Monthly revenue aggregation
-- - Order volume tracking
-- - Average order value metrics
-- ============================================================================

WITH orders AS (
    SELECT
        order_id,
        order_date,
        total_amount,
        status
    FROM {{ ref('stg_orders') }}
),

monthly_aggregates AS (
    SELECT
        YEAR(order_date) AS order_year,
        MONTH(order_date) AS order_month,
        TO_CHAR(order_date, 'YYYY-MM') AS order_year_month,
        
        -- Order counts
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT CASE WHEN status = 'completed' THEN order_id END) AS completed_orders,
        
        -- Revenue metrics
        SUM(total_amount) AS total_revenue,
        AVG(total_amount) AS avg_order_value
        
    FROM orders
    GROUP BY YEAR(order_date), MONTH(order_date), TO_CHAR(order_date, 'YYYY-MM')
)

SELECT
    -- Time dimensions
    order_year_month,
    order_year,
    order_month,
    
    -- Order metrics
    total_orders,
    completed_orders,
    
    -- Revenue metrics
    ROUND(total_revenue, 2) AS total_revenue,
    ROUND(avg_order_value, 2) AS avg_order_value

FROM monthly_aggregates
ORDER BY order_year DESC, order_month DESC

