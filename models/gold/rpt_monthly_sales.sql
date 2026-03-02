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
-- - Average order value calculation
-- - Month-over-month growth metrics
-- - Year-over-year comparisons
-- ============================================================================

WITH orders AS (
    SELECT 
        order_id,
        customer_id,
        order_date,
        order_status,
        total_amount,
        subtotal_amount,
        discount_amount,
        shipping_cost,
        tax_amount,
        is_completed,
        is_cancelled
    FROM {{ ref('stg_orders') }}
),

monthly_aggregates AS (
    SELECT
        -- Date dimensions
        DATE_TRUNC('month', order_date) AS order_month,
        YEAR(order_date) AS order_year,
        MONTH(order_date) AS order_month_num,
        TO_CHAR(order_date, 'YYYY-MM') AS year_month,
        TO_CHAR(order_date, 'Month YYYY') AS month_name,
        
        -- Order counts
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT CASE WHEN is_completed THEN order_id END) AS completed_orders,
        COUNT(DISTINCT CASE WHEN is_cancelled THEN order_id END) AS cancelled_orders,
        COUNT(DISTINCT CASE WHEN NOT is_completed AND NOT is_cancelled THEN order_id END) AS pending_orders,
        
        -- Customer metrics
        COUNT(DISTINCT customer_id) AS unique_customers,
        
        -- Revenue metrics (all orders)
        ROUND(SUM(total_amount), 2) AS total_revenue,
        ROUND(SUM(subtotal_amount), 2) AS subtotal_revenue,
        ROUND(SUM(discount_amount), 2) AS total_discounts,
        ROUND(SUM(shipping_cost), 2) AS total_shipping_revenue,
        ROUND(SUM(tax_amount), 2) AS total_tax_collected,
        
        -- Revenue metrics (completed orders only)
        ROUND(SUM(CASE WHEN is_completed THEN total_amount ELSE 0 END), 2) AS completed_revenue,
        ROUND(SUM(CASE WHEN is_completed THEN subtotal_amount ELSE 0 END), 2) AS completed_subtotal,
        
        -- Average order value
        ROUND(AVG(total_amount), 2) AS avg_order_value,
        ROUND(AVG(CASE WHEN is_completed THEN total_amount END), 2) AS avg_completed_order_value,
        
        -- Order value statistics
        ROUND(MIN(total_amount), 2) AS min_order_value,
        ROUND(MAX(total_amount), 2) AS max_order_value,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_amount), 2) AS median_order_value,
        
        -- Discount metrics
        ROUND(AVG(discount_amount), 2) AS avg_discount_per_order,
        COUNT(CASE WHEN discount_amount > 0 THEN 1 END) AS orders_with_discount,
        
        -- Calculate discount penetration rate
        CASE
            WHEN COUNT(DISTINCT order_id) > 0 THEN
                ROUND((COUNT(CASE WHEN discount_amount > 0 THEN 1 END)::FLOAT / COUNT(DISTINCT order_id)) * 100, 2)
            ELSE 0
        END AS discount_penetration_pct,
        
        -- Calculate average discount percentage
        CASE
            WHEN SUM(subtotal_amount + discount_amount) > 0 THEN
                ROUND((SUM(discount_amount) / SUM(subtotal_amount + discount_amount)) * 100, 2)
            ELSE 0
        END AS avg_discount_pct,
        
        -- Order completion rate
        CASE
            WHEN COUNT(DISTINCT order_id) > 0 THEN
                ROUND((COUNT(DISTINCT CASE WHEN is_completed THEN order_id END)::FLOAT / COUNT(DISTINCT order_id)) * 100, 2)
            ELSE 0
        END AS completion_rate_pct,
        
        -- Order cancellation rate
        CASE
            WHEN COUNT(DISTINCT order_id) > 0 THEN
                ROUND((COUNT(DISTINCT CASE WHEN is_cancelled THEN order_id END)::FLOAT / COUNT(DISTINCT order_id)) * 100, 2)
            ELSE 0
        END AS cancellation_rate_pct
        
    FROM orders
    GROUP BY 
        DATE_TRUNC('month', order_date),
        YEAR(order_date),
        MONTH(order_date),
        TO_CHAR(order_date, 'YYYY-MM'),
        TO_CHAR(order_date, 'Month YYYY')
),

with_growth_metrics AS (
    SELECT
        *,
        
        -- Calculate month-over-month growth
        LAG(total_revenue) OVER (ORDER BY order_month) AS prev_month_revenue,
        LAG(total_orders) OVER (ORDER BY order_month) AS prev_month_orders,
        LAG(unique_customers) OVER (ORDER BY order_month) AS prev_month_customers,
        
        -- Calculate year-over-year comparisons
        LAG(total_revenue, 12) OVER (ORDER BY order_month) AS prev_year_revenue,
        LAG(total_orders, 12) OVER (ORDER BY order_month) AS prev_year_orders,
        
        -- Calculate orders per customer
        CASE
            WHEN unique_customers > 0 THEN
                ROUND(total_orders::FLOAT / unique_customers, 2)
            ELSE 0
        END AS orders_per_customer,
        
        -- Calculate revenue per customer
        CASE
            WHEN unique_customers > 0 THEN
                ROUND(total_revenue::FLOAT / unique_customers, 2)
            ELSE 0
        END AS revenue_per_customer
        
    FROM monthly_aggregates
)

SELECT
    -- Date dimensions
    order_month,
    order_year,
    order_month_num,
    year_month,
    month_name,
    
    -- Order metrics
    total_orders,
    completed_orders,
    cancelled_orders,
    pending_orders,
    completion_rate_pct,
    cancellation_rate_pct,
    
    -- Customer metrics
    unique_customers,
    orders_per_customer,
    revenue_per_customer,
    
    -- Revenue metrics
    total_revenue,
    completed_revenue,
    subtotal_revenue,
    total_discounts,
    total_shipping_revenue,
    total_tax_collected,
    
    -- Average order value
    avg_order_value,
    avg_completed_order_value,
    min_order_value,
    max_order_value,
    median_order_value,
    
    -- Discount metrics
    avg_discount_per_order,
    avg_discount_pct,
    orders_with_discount,
    discount_penetration_pct,
    
    -- Month-over-month growth
    prev_month_revenue,
    CASE
        WHEN prev_month_revenue > 0 AND prev_month_revenue IS NOT NULL THEN
            ROUND(((total_revenue - prev_month_revenue) / prev_month_revenue) * 100, 2)
        ELSE NULL
    END AS mom_revenue_growth_pct,
    
    prev_month_orders,
    CASE
        WHEN prev_month_orders > 0 AND prev_month_orders IS NOT NULL THEN
            ROUND(((total_orders - prev_month_orders)::FLOAT / prev_month_orders) * 100, 2)
        ELSE NULL
    END AS mom_order_growth_pct,
    
    prev_month_customers,
    CASE
        WHEN prev_month_customers > 0 AND prev_month_customers IS NOT NULL THEN
            ROUND(((unique_customers - prev_month_customers)::FLOAT / prev_month_customers) * 100, 2)
        ELSE NULL
    END AS mom_customer_growth_pct,
    
    -- Year-over-year growth
    prev_year_revenue,
    CASE
        WHEN prev_year_revenue > 0 AND prev_year_revenue IS NOT NULL THEN
            ROUND(((total_revenue - prev_year_revenue) / prev_year_revenue) * 100, 2)
        ELSE NULL
    END AS yoy_revenue_growth_pct,
    
    prev_year_orders,
    CASE
        WHEN prev_year_orders > 0 AND prev_year_orders IS NOT NULL THEN
            ROUND(((total_orders - prev_year_orders)::FLOAT / prev_year_orders) * 100, 2)
        ELSE NULL
    END AS yoy_order_growth_pct,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM with_growth_metrics
ORDER BY order_month DESC
