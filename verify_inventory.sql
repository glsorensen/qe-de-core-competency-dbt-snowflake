-- ============================================================================
-- EXERCISE 5: Inventory & Supply Chain Analytics - Verification Queries
-- ============================================================================
-- These queries validate the Exercise 5 implementation and demonstrate
-- advanced dbt patterns: SCD Type 2, incremental models, window functions,
-- custom macros, and statistical anomaly detection.
-- ============================================================================

-- Query 1: Verify Supplier Performance Rankings
-- Shows supplier tier classification and performance metrics
SELECT
    supplier_name,
    supplier_tier,
    overall_performance_rank,
    composite_performance_score,
    reliability_score,
    lead_time_days,
    total_products_supplied,
    key_strength,
    key_weakness,
    recommended_action
FROM {{ ref('rpt_supplier_performance') }}
ORDER BY overall_performance_rank
LIMIT 5;

-- Query 2: Verify Inventory Health with Days of Supply
-- Demonstrates custom macro calculate_days_of_supply in action
SELECT
    product_name,
    quantity_available,
    avg_daily_sales,
    days_of_supply,
    inventory_health,
    stockout_risk_score,
    reorder_recommendation,
    value_at_risk_7_days
FROM {{ ref('rpt_inventory_health') }}
WHERE stockout_risk_score > 50
ORDER BY stockout_risk_score DESC
LIMIT 10;

-- Query 3: Verify SCD Type 2 Implementation
-- Shows how inventory health status changes are tracked over time
SELECT
    product_name,
    inventory_health,
    valid_from,
    valid_to,
    is_current,
    days_in_status,
    status_transition,
    quantity_available
FROM {{ ref('dim_inventory_history') }}
WHERE product_id = 1  -- Focus on one product for clarity
ORDER BY valid_from DESC;

-- Query 4: Verify Incremental Model (Inventory Movements)
-- Shows transaction-level facts with running inventory balance
SELECT
    transaction_date,
    product_name,
    transaction_type,
    quantity,
    running_inventory_balance,
    transaction_sequence,
    is_inbound,
    is_outbound,
    transaction_value
FROM {{ ref('fct_inventory_movements') }}
WHERE product_id = 1
ORDER BY transaction_date DESC, transaction_sequence DESC
LIMIT 15;

-- Query 5: Verify Window Functions - Week-over-Week Analysis
-- Shows LAG window function for time-series inventory analysis
SELECT
    product_name,
    snapshot_date,
    quantity_on_hand,
    previous_quantity_on_hand,
    wow_quantity_change,
    wow_change_percentage,
    inventory_health
FROM {{ ref('stg_inventory_snapshots') }}
WHERE product_id = 2
    AND snapshot_date >= '2024-10-01'
ORDER BY snapshot_date DESC;

-- Query 6: Verify Anomaly Detection (3-Sigma Rule)
-- Shows statistical outliers in inventory transactions
SELECT
    anomaly_id,
    product_name,
    anomaly_date,
    anomaly_type,
    severity,
    zscore,
    anomaly_description,
    recommended_action
FROM {{ ref('rpt_inventory_anomalies') }}
WHERE severity = 'High'
ORDER BY anomaly_date DESC
LIMIT 10;

-- Query 7: Verify Product-Supplier Relationships
-- Shows margin analysis and data quality checks
SELECT
    product_name,
    supplier_name,
    is_primary_supplier,
    unit_cost,
    product_retail_price,
    margin_percentage,
    cost_variance_from_retail,
    cost_exceeds_price_flag,
    lead_time_days
FROM {{ ref('stg_product_suppliers') }}
WHERE is_primary_supplier = TRUE
ORDER BY margin_percentage DESC;

-- Query 8: Verify Running Totals (Window Function)
-- Shows SUM window function for cumulative inventory balance
SELECT
    transaction_date,
    transaction_type,
    quantity,
    running_inventory_balance,
    transaction_sequence
FROM {{ ref('stg_inventory_transactions') }}
WHERE product_id = 3
ORDER BY transaction_date, transaction_sequence;

-- Query 9: Verify Supplier Dimension Enrichment
-- Shows aggregated metrics from product_suppliers
SELECT
    supplier_name,
    supplier_tier,
    country,
    lead_time_category,
    reliability_score,
    total_products_supplied,
    primary_products_count,
    avg_margin_percentage,
    is_active_contract,
    contract_days_remaining
FROM {{ ref('dim_suppliers') }}
ORDER BY supplier_tier, reliability_score DESC;

-- Query 10: Comprehensive Inventory Health Summary
-- Business-ready report showing inventory status across all products
SELECT
    inventory_health,
    COUNT(*) AS product_count,
    SUM(quantity_available) AS total_available_units,
    ROUND(AVG(days_of_supply), 1) AS avg_days_of_supply,
    ROUND(AVG(stockout_risk_score), 1) AS avg_risk_score,
    ROUND(SUM(inventory_value_at_retail), 2) AS total_inventory_value,
    ROUND(SUM(value_at_risk_7_days), 2) AS total_value_at_risk
FROM {{ ref('rpt_inventory_health') }}
GROUP BY inventory_health
ORDER BY
    CASE inventory_health
        WHEN 'critical' THEN 1
        WHEN 'low' THEN 2
        WHEN 'healthy' THEN 3
        WHEN 'overstocked' THEN 4
    END;

-- ============================================================================
-- ADVANCED PATTERN VERIFICATION
-- ============================================================================

-- Query 11: Verify SCD Type 2 Validity Periods (Using LEAD)
-- Shows how LEAD window function creates validity periods
SELECT
    product_name,
    inventory_health AS current_status,
    previous_inventory_health,
    valid_from,
    valid_to,
    days_in_status,
    CASE WHEN is_current THEN 'CURRENT' ELSE 'HISTORICAL' END AS record_type
FROM {{ ref('dim_inventory_history') }}
WHERE product_id IN (1, 2)
ORDER BY product_id, valid_from DESC;

-- Query 12: Verify Incremental Strategy (Merge Behavior)
-- Shows that incremental model only processes new transactions
-- Run this query before and after adding new transactions to see the difference
SELECT
    DATE(transaction_date) AS transaction_day,
    COUNT(*) AS transaction_count,
    SUM(CASE WHEN is_inbound THEN 1 ELSE 0 END) AS inbound_count,
    SUM(CASE WHEN is_outbound THEN 1 ELSE 0 END) AS outbound_count,
    SUM(transaction_value) AS total_value
FROM {{ ref('fct_inventory_movements') }}
GROUP BY DATE(transaction_date)
ORDER BY transaction_day DESC
LIMIT 10;

-- Query 13: Verify Custom Generic Test (Single Primary Supplier)
-- This test ensures each product has exactly one primary supplier
SELECT
    product_id,
    COUNT(*) AS primary_supplier_count
FROM {{ ref('stg_product_suppliers') }}
WHERE is_primary_supplier = TRUE
GROUP BY product_id
HAVING COUNT(*) != 1;
-- Should return 0 rows if test is passing

-- Query 14: Verify RANK() Window Function in Supplier Performance
-- Shows supplier rankings across multiple dimensions
SELECT
    supplier_name,
    reliability_rank,
    lead_time_rank,
    product_coverage_rank,
    margin_rank,
    overall_performance_rank,
    composite_performance_score
FROM {{ ref('rpt_supplier_performance') }}
ORDER BY overall_performance_rank;

-- Query 15: End-to-End Data Lineage Verification
-- Shows how data flows from Bronze → Silver → Gold
SELECT
    'Bronze' AS layer,
    'seeds' AS source,
    COUNT(*) AS row_count
FROM {{ source('raw', 'suppliers') }}

UNION ALL

SELECT
    'Silver' AS layer,
    'stg_suppliers' AS source,
    COUNT(*) AS row_count
FROM {{ ref('stg_suppliers') }}

UNION ALL

SELECT
    'Gold' AS layer,
    'dim_suppliers' AS source,
    COUNT(*) AS row_count
FROM {{ ref('dim_suppliers') }};

-- ============================================================================
-- Expected Results Summary:
-- ============================================================================
-- ✅ Query 1: 8 suppliers ranked by performance (Platinum → Bronze tiers)
-- ✅ Query 2: Products at high stockout risk with calculated days of supply
-- ✅ Query 3: Historical status changes for Product 1 with validity periods
-- ✅ Query 4: Transaction history with running inventory balance
-- ✅ Query 5: Week-over-week inventory changes using LAG window function
-- ✅ Query 6: Statistical anomalies detected using 3-sigma rule
-- ✅ Query 7: Product-supplier relationships with margin analysis
-- ✅ Query 8: Running totals demonstrating cumulative calculations
-- ✅ Query 9: Enriched supplier dimension with aggregated metrics
-- ✅ Query 10: Inventory health distribution across all products
-- ✅ Query 11: SCD Type 2 records with LEAD-generated validity periods
-- ✅ Query 12: Incremental model transaction summary by day
-- ✅ Query 13: Custom test validation (should return 0 rows)
-- ✅ Query 14: Multi-dimensional supplier rankings
-- ✅ Query 15: Data lineage verification (8 rows in each layer)
-- ============================================================================
