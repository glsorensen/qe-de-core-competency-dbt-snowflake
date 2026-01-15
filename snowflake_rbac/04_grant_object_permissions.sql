-- ============================================================================
-- Snowflake RBAC Setup: Grant Object-Level Permissions (OPTIONAL)
-- ============================================================================
-- This script grants explicit permissions on existing tables and views.
-- Note: If you ran the FUTURE GRANTS in previous scripts, this may not be
-- necessary. Use this script to backfill permissions on existing objects.
--
-- Run this script as SYSADMIN or SNOWCOMMERCE_ADMIN
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE SALES_DATABASE_MEAGAN;

-- ============================================================================
-- BRONZE LAYER (RAW Schema) - Grant access to existing tables
-- ============================================================================

USE SCHEMA RAW;

-- Engineer: SELECT on all existing seed tables
GRANT SELECT ON ALL TABLES IN SCHEMA RAW TO ROLE SNOWCOMMERCE_ENGINEER;

-- Example: Grant access to specific seed tables
-- GRANT SELECT ON TABLE RAW.CUSTOMERS TO ROLE SNOWCOMMERCE_ENGINEER;
-- GRANT SELECT ON TABLE RAW.PRODUCTS TO ROLE SNOWCOMMERCE_ENGINEER;
-- GRANT SELECT ON TABLE RAW.ORDERS TO ROLE SNOWCOMMERCE_ENGINEER;
-- GRANT SELECT ON TABLE RAW.ORDER_ITEMS TO ROLE SNOWCOMMERCE_ENGINEER;
-- GRANT SELECT ON TABLE RAW.MARKETING_CAMPAIGNS TO ROLE SNOWCOMMERCE_ENGINEER;
-- GRANT SELECT ON TABLE RAW.SUPPLIERS TO ROLE SNOWCOMMERCE_ENGINEER;
-- GRANT SELECT ON TABLE RAW.PRODUCT_SUPPLIERS TO ROLE SNOWCOMMERCE_ENGINEER;
-- GRANT SELECT ON TABLE RAW.INVENTORY_TRANSACTIONS TO ROLE SNOWCOMMERCE_ENGINEER;
-- GRANT SELECT ON TABLE RAW.INVENTORY_SNAPSHOTS TO ROLE SNOWCOMMERCE_ENGINEER;

-- ============================================================================
-- SILVER LAYER (SILVER Schema) - Grant access to existing staging models
-- ============================================================================

USE SCHEMA SILVER;

-- Engineer: Full access
GRANT SELECT ON ALL TABLES IN SCHEMA SILVER TO ROLE SNOWCOMMERCE_ENGINEER;
GRANT SELECT ON ALL VIEWS IN SCHEMA SILVER TO ROLE SNOWCOMMERCE_ENGINEER;

-- Analyst: Read access
GRANT SELECT ON ALL TABLES IN SCHEMA SILVER TO ROLE SNOWCOMMERCE_ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA SILVER TO ROLE SNOWCOMMERCE_ANALYST;

-- Example: Grant access to specific staging views
-- GRANT SELECT ON VIEW SILVER.STG_CUSTOMERS TO ROLE SNOWCOMMERCE_ANALYST;
-- GRANT SELECT ON VIEW SILVER.STG_PRODUCTS TO ROLE SNOWCOMMERCE_ANALYST;
-- GRANT SELECT ON VIEW SILVER.STG_ORDERS TO ROLE SNOWCOMMERCE_ANALYST;
-- GRANT SELECT ON VIEW SILVER.STG_ORDER_ITEMS TO ROLE SNOWCOMMERCE_ANALYST;
-- GRANT SELECT ON VIEW SILVER.STG_MARKETING_CAMPAIGNS TO ROLE SNOWCOMMERCE_ANALYST;
-- GRANT SELECT ON VIEW SILVER.STG_SUPPLIERS TO ROLE SNOWCOMMERCE_ANALYST;
-- GRANT SELECT ON VIEW SILVER.STG_PRODUCT_SUPPLIERS TO ROLE SNOWCOMMERCE_ANALYST;
-- GRANT SELECT ON VIEW SILVER.STG_INVENTORY_TRANSACTIONS TO ROLE SNOWCOMMERCE_ANALYST;
-- GRANT SELECT ON VIEW SILVER.STG_INVENTORY_SNAPSHOTS TO ROLE SNOWCOMMERCE_ANALYST;

-- ============================================================================
-- GOLD LAYER (GOLD Schema) - Grant access to existing dimensions/facts/reports
-- ============================================================================

USE SCHEMA GOLD;

-- Engineer: Full access
GRANT SELECT ON ALL TABLES IN SCHEMA GOLD TO ROLE SNOWCOMMERCE_ENGINEER;
GRANT SELECT ON ALL VIEWS IN SCHEMA GOLD TO ROLE SNOWCOMMERCE_ENGINEER;

-- Analyst: Read access
GRANT SELECT ON ALL TABLES IN SCHEMA GOLD TO ROLE SNOWCOMMERCE_ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA GOLD TO ROLE SNOWCOMMERCE_ANALYST;

-- Viewer: Read access
GRANT SELECT ON ALL TABLES IN SCHEMA GOLD TO ROLE SNOWCOMMERCE_VIEWER;
GRANT SELECT ON ALL VIEWS IN SCHEMA GOLD TO ROLE SNOWCOMMERCE_VIEWER;

-- Example: Grant access to specific gold layer models
-- Dimensions
-- GRANT SELECT ON TABLE GOLD.DIM_CUSTOMERS TO ROLE SNOWCOMMERCE_VIEWER;
-- GRANT SELECT ON TABLE GOLD.DIM_SUPPLIERS TO ROLE SNOWCOMMERCE_VIEWER;
-- GRANT SELECT ON TABLE GOLD.DIM_INVENTORY_HISTORY TO ROLE SNOWCOMMERCE_VIEWER;

-- Facts
-- GRANT SELECT ON TABLE GOLD.FCT_ORDERS TO ROLE SNOWCOMMERCE_VIEWER;
-- GRANT SELECT ON TABLE GOLD.FCT_ORDER_ITEMS TO ROLE SNOWCOMMERCE_VIEWER;
-- GRANT SELECT ON TABLE GOLD.FCT_INVENTORY_MOVEMENTS TO ROLE SNOWCOMMERCE_VIEWER;

-- Reports
-- GRANT SELECT ON TABLE GOLD.RPT_CUSTOMER_METRICS TO ROLE SNOWCOMMERCE_VIEWER;
-- GRANT SELECT ON TABLE GOLD.RPT_PRODUCT_PERFORMANCE TO ROLE SNOWCOMMERCE_VIEWER;
-- GRANT SELECT ON TABLE GOLD.RPT_INVENTORY_HEALTH TO ROLE SNOWCOMMERCE_VIEWER;
-- GRANT SELECT ON TABLE GOLD.RPT_SUPPLIER_PERFORMANCE TO ROLE SNOWCOMMERCE_VIEWER;
-- GRANT SELECT ON TABLE GOLD.RPT_INVENTORY_ANOMALIES TO ROLE SNOWCOMMERCE_VIEWER;

-- ============================================================================
-- ADVANCED: Row-Level Security (RLS) Example
-- ============================================================================
-- If you need to restrict access to specific rows (e.g., region-based access),
-- you can create secure views with WHERE clauses or use Snowflake's Row Access Policies

-- Example: Create a secure view that filters data by user context
-- CREATE OR REPLACE SECURE VIEW GOLD.RPT_CUSTOMER_METRICS_SECURE AS
-- SELECT *
-- FROM GOLD.RPT_CUSTOMER_METRICS
-- WHERE 
--   CASE 
--     WHEN CURRENT_ROLE() = 'SNOWCOMMERCE_VIEWER_NA' THEN country IN ('USA', 'Canada')
--     WHEN CURRENT_ROLE() = 'SNOWCOMMERCE_VIEWER_EU' THEN country IN ('UK', 'France', 'Germany')
--     ELSE TRUE  -- Admins and Engineers see all
--   END;

-- ============================================================================
-- Verify object-level grants
-- ============================================================================
SHOW GRANTS ON ALL TABLES IN SCHEMA RAW;
SHOW GRANTS ON ALL TABLES IN SCHEMA SILVER;
SHOW GRANTS ON ALL VIEWS IN SCHEMA SILVER;
SHOW GRANTS ON ALL TABLES IN SCHEMA GOLD;
SHOW GRANTS ON ALL VIEWS IN SCHEMA GOLD;

-- ============================================================================
-- NEXT STEPS:
-- Run 05_assign_roles_to_users.sql to assign roles to team members
-- ============================================================================
