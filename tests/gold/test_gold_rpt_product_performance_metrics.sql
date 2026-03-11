-- Gold Layer: Product Performance Report Accuracy
-- Validates that performance tier classification is accurate based on revenue

SELECT
    product_id,
    product_name,
    total_revenue,
    performance_tier
FROM {{ ref('rpt_product_performance') }}
WHERE
    (total_revenue >= 1000 AND performance_tier != 'top_performer')
    OR (total_revenue >= 500 AND total_revenue < 1000 AND performance_tier != 'strong_performer')
    OR (total_revenue >= 100 AND total_revenue < 500 AND performance_tier != 'moderate_performer')
    OR (total_revenue < 100 AND performance_tier != 'low_performer')
