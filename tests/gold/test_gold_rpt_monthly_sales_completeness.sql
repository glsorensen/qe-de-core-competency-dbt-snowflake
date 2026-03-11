-- Gold Layer: Monthly Sales Report Completeness
-- Validates that required fields are populated

SELECT
    order_year_month,
    order_year,
    order_month,
    total_orders,
    completed_orders,
    total_revenue,
    avg_order_value
FROM {{ ref('rpt_monthly_sales') }}
WHERE
    order_year_month IS NULL
    OR total_orders IS NULL
    OR total_revenue IS NULL
    OR avg_order_value IS NULL
