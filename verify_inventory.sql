-- ============================================================================
-- Exercise 5: Inventory & Supply Chain Analytics - Verification Queries
-- ============================================================================
-- Run these queries in Snowflake to verify your Exercise 5 implementation
-- Make sure to use the correct database and schema (e.g., USE SCHEMA gold;)
--
-- This exercise demonstrates advanced dbt patterns:
-- - SCD Type 2 (Slowly Changing Dimensions)
-- - Incremental models with merge strategy
-- - Window functions (LAG, LEAD, SUM, RANK, ROW_NUMBER)
-- - Custom macros (calculate_days_of_supply)
-- - Statistical anomaly detection (3-sigma rule)
-- - Custom generic tests
-- ============================================================================

-- Query 1: Verify Supplier Performance Rankings
-- Shows supplier tier classification and performance metrics using RANK() window function
SELECT
    supplier_name,
    supplier_tier,
    overall_performance_rank,
    ROUND(composite_performance_score, 2) AS performance_score,
    reliability_score,
    lead_time_days,
    lead_time_category,
    total_products_supplied,
    key_strength,
    key_weakness,
    recommended_action
FROM gold.rpt_supplier_performance
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
    ROUND(value_at_risk_7_days, 2) AS value_at_risk
FROM gold.rpt_inventory_health
WHERE stockout_risk_score > 50
ORDER BY stockout_risk_score DESC
LIMIT 10;

-- Query 3: Verify SCD Type 2 Implementation (Using LEAD Window Function)
-- Shows how inventory health status changes are tracked over time with validity periods
SELECT
    product_name,
    inventory_health,
    TO_CHAR(valid_from, 'YYYY-MM-DD') AS valid_from,
    TO_CHAR(valid_to, 'YYYY-MM-DD') AS valid_to,
    is_current,
    days_in_status,
    status_transition,
    quantity_available
FROM gold.dim_inventory_history
WHERE product_id = 1  -- Focus on one product for clarity
ORDER BY valid_from DESC
LIMIT 10;

-- Query 4: Verify Incremental Model (Inventory Movements)
-- Shows transaction-level facts with running inventory balance using SUM window function
SELECT
    TO_CHAR(transaction_date, 'YYYY-MM-DD') AS transaction_date,
    product_name,
    transaction_type,
    quantity,
    running_inventory_balance,
    transaction_sequence,
    is_inbound,
    is_outbound,
    ROUND(transaction_value, 2) AS transaction_value
FROM gold.fct_inventory_movements
WHERE product_id = 1
ORDER BY transaction_date DESC, transaction_sequence DESC
LIMIT 15;

-- Query 5: Verify Window Functions - Week-over-Week Analysis
-- Shows LAG window function for time-series inventory analysis
SELECT
    product_name,
    TO_CHAR(snapshot_date, 'YYYY-MM-DD') AS snapshot_date,
    quantity_on_hand,
    previous_quantity_on_hand,
    wow_quantity_change,
    ROUND(wow_change_percentage, 2) AS wow_change_pct,
    inventory_health
FROM silver.stg_inventory_snapshots
WHERE product_id = 2
    AND snapshot_date >= '2024-10-01'
ORDER BY snapshot_date DESC
LIMIT 10;

-- Query 6: Verify Anomaly Detection (3-Sigma Rule)
-- Shows statistical outliers in inventory transactions
SELECT
    anomaly_id,
    product_name,
    TO_CHAR(anomaly_date, 'YYYY-MM-DD') AS anomaly_date,
    anomaly_type,
    severity,
    ROUND(zscore, 2) AS z_score,
    anomaly_description,
    recommended_action
FROM gold.rpt_inventory_anomalies
WHERE severity = 'High'
ORDER BY anomaly_date DESC
LIMIT 10;

-- Query 7: Verify Product-Supplier Relationships
-- Shows margin analysis and data quality checks
SELECT
    product_name,
    supplier_name,
    is_primary_supplier,
    ROUND(unit_cost, 2) AS unit_cost,
    ROUND(product_retail_price, 2) AS retail_price,
    ROUND(margin_percentage, 2) AS margin_pct,
    ROUND(cost_variance_from_retail, 2) AS cost_variance,
    cost_exceeds_price_flag,
    lead_time_days
FROM silver.stg_product_suppliers
WHERE is_primary_supplier = TRUE
ORDER BY margin_percentage DESC
LIMIT 10;

-- Query 8: Verify Running Totals (SUM Window Function)
-- Shows cumulative inventory balance calculation
SELECT
    TO_CHAR(transaction_date, 'YYYY-MM-DD') AS transaction_date,
    transaction_type,
    quantity,
    running_inventory_balance,
    transaction_sequence
FROM silver.stg_inventory_transactions
WHERE product_id = 3
ORDER BY transaction_date, transaction_sequence
LIMIT 15;

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
    ROUND(avg_margin_percentage, 2) AS avg_margin_pct,
    is_active_contract,
    contract_days_remaining
FROM gold.dim_suppliers
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
FROM gold.rpt_inventory_health
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

-- Query 11: Verify SCD Type 2 Validity Periods (Generated by LEAD Window Function)
-- Shows how LEAD window function creates validity periods for historical tracking
SELECT
    product_name,
    inventory_health AS current_status,
    previous_inventory_health,
    TO_CHAR(valid_from, 'YYYY-MM-DD') AS valid_from,
    TO_CHAR(valid_to, 'YYYY-MM-DD') AS valid_to,
    days_in_status,
    CASE WHEN is_current THEN 'CURRENT' ELSE 'HISTORICAL' END AS record_type,
    status_transition
FROM gold.dim_inventory_history
WHERE product_id IN (1, 2)
ORDER BY product_id, valid_from DESC
LIMIT 20;

-- Query 12: Verify Incremental Strategy (Merge Behavior)
-- Shows that incremental model processes transactions efficiently
SELECT
    DATE(transaction_date) AS transaction_day,
    COUNT(*) AS transaction_count,
    SUM(CASE WHEN is_inbound THEN 1 ELSE 0 END) AS inbound_count,
    SUM(CASE WHEN is_outbound THEN 1 ELSE 0 END) AS outbound_count,
    ROUND(SUM(transaction_value), 2) AS total_value
FROM gold.fct_inventory_movements
GROUP BY DATE(transaction_date)
ORDER BY transaction_day DESC
LIMIT 10;

-- Query 13: Verify Custom Generic Test (Single Primary Supplier)
-- This test ensures each product has exactly one primary supplier
SELECT
    product_id,
    COUNT(*) AS primary_supplier_count
FROM silver.stg_product_suppliers
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
    ROUND(composite_performance_score, 2) AS performance_score
FROM gold.rpt_supplier_performance
ORDER BY overall_performance_rank;

-- Query 15: End-to-End Data Lineage Verification
-- Shows how data flows from Bronze → Silver → Gold (Medallion Architecture)
SELECT
    'Bronze (seeds)' AS layer,
    'suppliers' AS table_name,
    COUNT(*) AS row_count
FROM raw.suppliers

UNION ALL

SELECT
    'Silver (staging)' AS layer,
    'stg_suppliers' AS table_name,
    COUNT(*) AS row_count
FROM silver.stg_suppliers

UNION ALL

SELECT
    'Gold (dimension)' AS layer,
    'dim_suppliers' AS table_name,
    COUNT(*) AS row_count
FROM gold.dim_suppliers;

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
