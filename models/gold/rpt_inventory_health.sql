{{
    config(
        materialized='table',
        tags=['report', 'inventory', 'exercise5']
    )
}}

/*
    Inventory Health Report
    =======================
    Current inventory status with days of supply and risk scoring.
    
    Business Logic:
    - Uses custom macro calculate_days_of_supply to estimate runway
    - Calculates stockout risk based on velocity and current levels
    - Provides value at risk for low inventory situations
    - Aggregates recent sales velocity from inventory movements
    
    Grain: One row per product (current status only)
*/

WITH current_inventory AS (
    SELECT
        product_id,
        quantity_on_hand,
        quantity_reserved,
        quantity_available,
        reorder_point,
        reorder_quantity,
        inventory_health,
        is_below_reorder_point,
        is_stockout,
        snapshot_date AS last_snapshot_date,
        snapshot_age_days
    FROM {{ ref('stg_inventory_snapshots') }}
    WHERE is_current_snapshot = TRUE
),

-- Calculate sales velocity from recent transactions (last 30 days)
recent_sales_velocity AS (
    SELECT
        product_id,
        COUNT(*) AS total_sale_transactions,
        SUM(ABS(quantity)) AS total_units_sold,
        ROUND(SUM(ABS(quantity)) / NULLIF(COUNT(DISTINCT DATE(transaction_date)), 0), 2) AS avg_daily_sales,
        MIN(transaction_date) AS first_sale_date,
        MAX(transaction_date) AS last_sale_date
    FROM {{ ref('fct_inventory_movements') }}
    WHERE is_outbound = TRUE
        AND transaction_type = 'sale'
        AND transaction_date >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY product_id
),

-- Get product details and pricing
products AS (
    SELECT
        product_id,
        product_name,
        category_id,
        price AS retail_price,
        cost
    FROM {{ ref('stg_products') }}
),

-- Combine metrics
inventory_metrics AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category_id,
        p.retail_price,
        p.cost,
        
        -- Current inventory levels
        COALESCE(ci.quantity_on_hand, 0) AS quantity_on_hand,
        COALESCE(ci.quantity_reserved, 0) AS quantity_reserved,
        COALESCE(ci.quantity_available, 0) AS quantity_available,
        ci.reorder_point,
        ci.reorder_quantity,
        ci.inventory_health,
        ci.is_below_reorder_point,
        ci.is_stockout,
        ci.last_snapshot_date,
        ci.snapshot_age_days,
        
        -- Sales velocity metrics
        COALESCE(rsv.total_units_sold, 0) AS units_sold_last_30_days,
        COALESCE(rsv.avg_daily_sales, 0) AS avg_daily_sales,
        rsv.last_sale_date,
        
        -- Days of supply calculation using custom macro
        {{ calculate_days_of_supply('ci.quantity_available', 'rsv.avg_daily_sales') }} AS days_of_supply,
        
        -- Inventory value metrics
        ROUND(ci.quantity_available * p.retail_price, 2) AS inventory_value_at_retail,
        ROUND(ci.quantity_available * p.cost, 2) AS inventory_value_at_cost,
        
        -- Value at risk calculations
        CASE 
            WHEN ci.is_stockout THEN ROUND(rsv.avg_daily_sales * 7 * p.retail_price, 2)
            WHEN ci.is_below_reorder_point THEN ROUND(
                (ci.reorder_point - ci.quantity_available) * p.retail_price, 
                2
            )
            ELSE 0
        END AS value_at_risk_7_days
        
    FROM products p
    LEFT JOIN current_inventory ci ON p.product_id = ci.product_id
    LEFT JOIN recent_sales_velocity rsv ON p.product_id = rsv.product_id
),

-- Add risk scoring
final AS (
    SELECT
        *,
        
        -- Stockout risk score (0-100)
        CASE
            WHEN is_stockout THEN 100
            WHEN days_of_supply IS NULL OR avg_daily_sales = 0 THEN 0
            WHEN days_of_supply <= 3 THEN 90
            WHEN days_of_supply <= 7 THEN 70
            WHEN days_of_supply <= 14 THEN 40
            WHEN days_of_supply <= 30 THEN 20
            ELSE 10
        END AS stockout_risk_score,
        
        -- Reorder recommendation
        CASE
            WHEN is_stockout THEN 'URGENT: Out of stock'
            WHEN is_below_reorder_point THEN 'REORDER: Below reorder point'
            WHEN days_of_supply IS NOT NULL AND days_of_supply <= 7 THEN 'WARNING: Less than 1 week supply'
            WHEN days_of_supply IS NOT NULL AND days_of_supply <= 14 THEN 'MONITOR: Less than 2 weeks supply'
            WHEN inventory_health = 'overstocked' THEN 'EXCESS: Consider reducing stock'
            ELSE 'HEALTHY: No action needed'
        END AS reorder_recommendation,
        
        -- Suggested order quantity
        CASE
            WHEN is_below_reorder_point OR is_stockout THEN reorder_quantity
            ELSE 0
        END AS suggested_order_quantity,
        
        -- Audit timestamp
        CURRENT_TIMESTAMP() AS dbt_updated_at
        
    FROM inventory_metrics
)

SELECT * FROM final
ORDER BY stockout_risk_score DESC, product_name
