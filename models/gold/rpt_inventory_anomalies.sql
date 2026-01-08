{{
    config(
        materialized='table',
        tags=['report', 'inventory', 'anomaly_detection', 'exercise5']
    )
}}

/*
    Inventory Anomalies Report
    ==========================
    Statistical anomaly detection for inventory movements and levels.
    
    Business Logic:
    - Uses 3-sigma rule for statistical anomaly detection
    - Identifies unusual transaction patterns (quantity, frequency)
    - Flags significant deviations from historical norms
    - Detects potential data quality issues or business events
    
    Grain: One row per detected anomaly
*/

WITH product_transaction_stats AS (
    -- Calculate statistical measures for each product's transactions
    SELECT
        product_id,
        COUNT(*) AS total_transactions,
        ROUND(AVG(ABS(quantity)), 2) AS avg_quantity,
        ROUND(STDDEV(ABS(quantity)), 2) AS stddev_quantity,
        ROUND(AVG(transaction_value), 2) AS avg_transaction_value,
        ROUND(STDDEV(transaction_value), 2) AS stddev_transaction_value,
        MIN(transaction_date) AS first_transaction_date,
        MAX(transaction_date) AS last_transaction_date
    FROM {{ ref('fct_inventory_movements') }}
    WHERE transaction_value IS NOT NULL
    GROUP BY product_id
),

-- Identify transactions that are statistical outliers (3-sigma rule)
quantity_anomalies AS (
    SELECT
        t.transaction_id,
        t.product_id,
        t.product_name,
        t.transaction_date,
        t.transaction_type,
        t.quantity,
        t.transaction_value,
        s.avg_quantity,
        s.stddev_quantity,
        
        -- Calculate z-score for quantity
        CASE 
            WHEN s.stddev_quantity > 0 THEN
                ROUND((ABS(t.quantity) - s.avg_quantity) / NULLIF(s.stddev_quantity, 0), 2)
            ELSE 0
        END AS quantity_zscore,
        
        -- Flag anomaly if z-score > 3 (99.7% confidence)
        CASE 
            WHEN s.stddev_quantity > 0 
                AND ABS((ABS(t.quantity) - s.avg_quantity) / NULLIF(s.stddev_quantity, 0)) > 3 
            THEN TRUE
            ELSE FALSE
        END AS is_quantity_anomaly,
        
        'Unusual transaction quantity' AS anomaly_type,
        
        CONCAT(
            'Transaction quantity (', ABS(t.quantity), ') is ',
            ROUND(ABS((ABS(t.quantity) - s.avg_quantity) / NULLIF(s.avg_quantity, 0)) * 100, 1),
            '% different from average (', s.avg_quantity, '). Z-score: ',
            ROUND((ABS(t.quantity) - s.avg_quantity) / NULLIF(s.stddev_quantity, 0), 2)
        ) AS anomaly_description
        
    FROM {{ ref('fct_inventory_movements') }} t
    INNER JOIN product_transaction_stats s ON t.product_id = s.product_id
    WHERE s.stddev_quantity > 0
        AND ABS((ABS(t.quantity) - s.avg_quantity) / NULLIF(s.stddev_quantity, 0)) > 3
),

-- Detect unusual value transactions
value_anomalies AS (
    SELECT
        t.transaction_id,
        t.product_id,
        t.product_name,
        t.transaction_date,
        t.transaction_type,
        t.quantity,
        t.transaction_value,
        s.avg_transaction_value,
        s.stddev_transaction_value,
        
        -- Calculate z-score for transaction value
        CASE 
            WHEN s.stddev_transaction_value > 0 THEN
                ROUND((t.transaction_value - s.avg_transaction_value) / NULLIF(s.stddev_transaction_value, 0), 2)
            ELSE 0
        END AS value_zscore,
        
        CASE 
            WHEN s.stddev_transaction_value > 0 
                AND ABS((t.transaction_value - s.avg_transaction_value) / NULLIF(s.stddev_transaction_value, 0)) > 3 
            THEN TRUE
            ELSE FALSE
        END AS is_value_anomaly,
        
        'Unusual transaction value' AS anomaly_type,
        
        CONCAT(
            'Transaction value ($', t.transaction_value, ') is ',
            ROUND(ABS((t.transaction_value - s.avg_transaction_value) / NULLIF(s.avg_transaction_value, 0)) * 100, 1),
            '% different from average ($', s.avg_transaction_value, '). Z-score: ',
            ROUND((t.transaction_value - s.avg_transaction_value) / NULLIF(s.stddev_transaction_value, 0), 2)
        ) AS anomaly_description
        
    FROM {{ ref('fct_inventory_movements') }} t
    INNER JOIN product_transaction_stats s ON t.product_id = s.product_id
    WHERE s.stddev_transaction_value > 0
        AND ABS((t.transaction_value - s.avg_transaction_value) / NULLIF(s.stddev_transaction_value, 0)) > 3
),

-- Detect rapid status changes in inventory health (from SCD Type 2)
frequent_status_changes AS (
    SELECT
        product_id,
        product_name,
        COUNT(*) AS status_change_count,
        MIN(valid_from) AS first_change_date,
        MAX(valid_from) AS last_change_date,
        DATEDIFF('day', MIN(valid_from), MAX(valid_from)) AS days_span,
        ROUND(COUNT(*) / NULLIF(DATEDIFF('day', MIN(valid_from), MAX(valid_from)), 0), 2) AS changes_per_day,
        
        'Frequent inventory status changes' AS anomaly_type,
        
        CONCAT(
            'Inventory health status changed ', COUNT(*), ' times in ',
            DATEDIFF('day', MIN(valid_from), MAX(valid_from)), ' days (',
            ROUND(COUNT(*) / NULLIF(DATEDIFF('day', MIN(valid_from), MAX(valid_from)), 0), 2),
            ' changes per day)'
        ) AS anomaly_description
        
    FROM {{ ref('dim_inventory_history') }}
    WHERE valid_from >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY product_id, product_name
    HAVING COUNT(*) > 5  -- More than 5 status changes in period
        AND DATEDIFF('day', MIN(valid_from), MAX(valid_from)) > 0
),

-- Union all anomaly types
all_anomalies AS (
    SELECT
        transaction_id,
        product_id,
        product_name,
        transaction_date AS anomaly_date,
        transaction_type,
        quantity,
        transaction_value,
        anomaly_type,
        anomaly_description,
        quantity_zscore AS zscore,
        'High' AS severity
    FROM quantity_anomalies
    WHERE is_quantity_anomaly = TRUE
    
    UNION ALL
    
    SELECT
        transaction_id,
        product_id,
        product_name,
        transaction_date AS anomaly_date,
        transaction_type,
        quantity,
        transaction_value,
        anomaly_type,
        anomaly_description,
        value_zscore AS zscore,
        'High' AS severity
    FROM value_anomalies
    WHERE is_value_anomaly = TRUE
    
    UNION ALL
    
    SELECT
        NULL AS transaction_id,
        product_id,
        product_name,
        last_change_date AS anomaly_date,
        NULL AS transaction_type,
        NULL AS quantity,
        NULL AS transaction_value,
        anomaly_type,
        anomaly_description,
        changes_per_day AS zscore,
        CASE 
            WHEN changes_per_day >= 0.5 THEN 'High'
            WHEN changes_per_day >= 0.3 THEN 'Medium'
            ELSE 'Low'
        END AS severity
    FROM frequent_status_changes
),

-- Add context and recommendations
final AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY anomaly_date DESC, severity DESC) AS anomaly_id,
        *,
        
        -- Days since anomaly
        DATEDIFF('day', anomaly_date, CURRENT_DATE()) AS days_since_anomaly,
        
        -- Recommended action based on anomaly type and severity
        CASE
            WHEN anomaly_type = 'Unusual transaction quantity' AND severity = 'High' 
                THEN 'Verify transaction accuracy and investigate cause'
            WHEN anomaly_type = 'Unusual transaction value' AND severity = 'High' 
                THEN 'Review pricing and cost data for accuracy'
            WHEN anomaly_type = 'Frequent inventory status changes' AND severity = 'High' 
                THEN 'Investigate inventory volatility and demand patterns'
            WHEN anomaly_type = 'Frequent inventory status changes' AND severity = 'Medium' 
                THEN 'Monitor for continued instability'
            ELSE 'Review for potential data quality issues'
        END AS recommended_action,
        
        -- Audit timestamp
        CURRENT_TIMESTAMP() AS dbt_updated_at
        
    FROM all_anomalies
)

SELECT * FROM final
ORDER BY severity DESC, anomaly_date DESC
