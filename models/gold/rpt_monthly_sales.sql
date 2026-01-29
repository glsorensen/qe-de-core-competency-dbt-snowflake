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
-- - Month-over-month growth analysis
-- ============================================================================

WITH orders AS (
    SELECT
        order_id,
        order_date,
        order_year,
        order_month,
        order_year_month,
        total_amount,
        is_completed,
        is_cancelled
    FROM {{ ref('stg_orders') }}
),

monthly_aggregates AS (
    SELECT
        order_year,
        order_month,
        order_year_month,
        
        -- Order counts
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT CASE WHEN is_completed THEN order_id END) AS completed_orders,
        COUNT(DISTINCT CASE WHEN is_cancelled THEN order_id END) AS cancelled_orders,
        
        -- Revenue metrics
        SUM(total_amount) AS total_revenue,
        SUM(CASE WHEN is_completed THEN total_amount ELSE 0 END) AS completed_revenue,
        AVG(total_amount) AS avg_order_value,
        AVG(CASE WHEN is_completed THEN total_amount END) AS avg_completed_order_value,
        
        -- Min/Max
        MIN(total_amount) AS min_order_value,
        MAX(total_amount) AS max_order_value,
        
        -- Order dates
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date
        
    FROM orders
    GROUP BY order_year, order_month, order_year_month
)

SELECT
    -- Time dimensions
    order_year,
    order_month,
    order_year_month,
    first_order_date,
    last_order_date,
    
    -- Order metrics
    total_orders,
    completed_orders,
    cancelled_orders,
    
    -- Revenue metrics
    ROUND(total_revenue, 2) AS total_revenue,
    ROUND(completed_revenue, 2) AS completed_revenue,
    ROUND(avg_order_value, 2) AS avg_order_value,
    ROUND(avg_completed_order_value, 2) AS avg_completed_order_value,
    ROUND(min_order_value, 2) AS min_order_value,
    ROUND(max_order_value, 2) AS max_order_value,
    
    -- Performance rates
    CASE
        WHEN total_orders > 0 THEN
            ROUND((completed_orders::FLOAT / total_orders) * 100, 2)
        ELSE 0
    END AS completion_rate_pct,
    
    CASE
        WHEN total_orders > 0 THEN
            ROUND((cancelled_orders::FLOAT / total_orders) * 100, 2)
        ELSE 0
    END AS cancellation_rate_pct,
    
    -- Month-over-month growth (calculated via window functions)
    LAG(total_revenue) OVER (ORDER BY order_year, order_month) AS prev_month_revenue,
    
    CASE
        WHEN LAG(total_revenue) OVER (ORDER BY order_year, order_month) > 0 THEN
            ROUND(
                ((total_revenue - LAG(total_revenue) OVER (ORDER BY order_year, order_month)) 
                / LAG(total_revenue) OVER (ORDER BY order_year, order_month)) * 100,
                2
            )
        ELSE NULL
    END AS revenue_growth_pct,
    
    LAG(total_orders) OVER (ORDER BY order_year, order_month) AS prev_month_orders,
    
    CASE
        WHEN LAG(total_orders) OVER (ORDER BY order_year, order_month) > 0 THEN
            ROUND(
                ((total_orders - LAG(total_orders) OVER (ORDER BY order_year, order_month))::FLOAT 
                / LAG(total_orders) OVER (ORDER BY order_year, order_month)) * 100,
                2
            )
        ELSE NULL
    END AS order_growth_pct,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM monthly_aggregates
ORDER BY order_year DESC, order_month DESC
