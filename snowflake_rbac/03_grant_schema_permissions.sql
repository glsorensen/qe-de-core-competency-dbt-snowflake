-- ============================================================================
-- Snowflake RBAC Setup: Grant Schema-Level Permissions
-- ============================================================================
-- This script grants schema-level privileges following Medallion Architecture:
-- - Bronze Layer (RAW schema): Raw data sources
-- - Silver Layer (SILVER schema): Cleaned staging models
-- - Gold Layer (GOLD schema): Business-ready analytics
--
-- Access Pattern:
-- - Engineers: Full access to all layers (build pipelines)
-- - Analysts: Read access to SILVER and GOLD (query cleaned data)
-- - Viewers: Read access to GOLD only (business reports)
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE SALES_DATABASE_MEAGAN;

-- ============================================================================
-- BRONZE LAYER (RAW Schema) - Raw Data Sources
-- ============================================================================

-- ADMIN: Full control
GRANT ALL PRIVILEGES ON SCHEMA RAW TO ROLE SNOWCOMMERCE_ADMIN;

-- ENGINEER: Full access (load seeds, create sources)
GRANT USAGE ON SCHEMA RAW TO ROLE SNOWCOMMERCE_ENGINEER;
GRANT CREATE TABLE ON SCHEMA RAW TO ROLE SNOWCOMMERCE_ENGINEER;
GRANT CREATE VIEW ON SCHEMA RAW TO ROLE SNOWCOMMERCE_ENGINEER;
GRANT SELECT ON ALL TABLES IN SCHEMA RAW TO ROLE SNOWCOMMERCE_ENGINEER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA RAW TO ROLE SNOWCOMMERCE_ENGINEER;

-- ANALYST: No direct access to raw data (must use staging models)
-- VIEWER: No access to raw data

-- ============================================================================
-- SILVER LAYER (SILVER Schema) - Cleaned Staging Models
-- ============================================================================

-- Create SILVER schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS SILVER
    COMMENT = 'Silver layer - cleaned staging models (stg_*)';

-- ADMIN: Full control
GRANT ALL PRIVILEGES ON SCHEMA SILVER TO ROLE SNOWCOMMERCE_ADMIN;

-- ENGINEER: Full access (build staging models)
GRANT USAGE ON SCHEMA SILVER TO ROLE SNOWCOMMERCE_ENGINEER;
GRANT CREATE TABLE ON SCHEMA SILVER TO ROLE SNOWCOMMERCE_ENGINEER;
GRANT CREATE VIEW ON SCHEMA SILVER TO ROLE SNOWCOMMERCE_ENGINEER;
GRANT SELECT ON ALL TABLES IN SCHEMA SILVER TO ROLE SNOWCOMMERCE_ENGINEER;
GRANT SELECT ON ALL VIEWS IN SCHEMA SILVER TO ROLE SNOWCOMMERCE_ENGINEER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA SILVER TO ROLE SNOWCOMMERCE_ENGINEER;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA SILVER TO ROLE SNOWCOMMERCE_ENGINEER;

-- ANALYST: Read access (query cleaned data)
GRANT USAGE ON SCHEMA SILVER TO ROLE SNOWCOMMERCE_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA SILVER TO ROLE SNOWCOMMERCE_ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA SILVER TO ROLE SNOWCOMMERCE_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA SILVER TO ROLE SNOWCOMMERCE_ANALYST;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA SILVER TO ROLE SNOWCOMMERCE_ANALYST;

-- VIEWER: No access to staging layer (should use gold layer instead)

-- ============================================================================
-- GOLD LAYER (GOLD Schema) - Business-Ready Analytics
-- ============================================================================

-- Create GOLD schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS GOLD
    COMMENT = 'Gold layer - business-ready models (dim_*, fct_*, rpt_*)';

-- ADMIN: Full control
GRANT ALL PRIVILEGES ON SCHEMA GOLD TO ROLE SNOWCOMMERCE_ADMIN;

-- ENGINEER: Full access (build dimensions, facts, reports)
GRANT USAGE ON SCHEMA GOLD TO ROLE SNOWCOMMERCE_ENGINEER;
GRANT CREATE TABLE ON SCHEMA GOLD TO ROLE SNOWCOMMERCE_ENGINEER;
GRANT CREATE VIEW ON SCHEMA GOLD TO ROLE SNOWCOMMERCE_ENGINEER;
GRANT SELECT ON ALL TABLES IN SCHEMA GOLD TO ROLE SNOWCOMMERCE_ENGINEER;
GRANT SELECT ON ALL VIEWS IN SCHEMA GOLD TO ROLE SNOWCOMMERCE_ENGINEER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA GOLD TO ROLE SNOWCOMMERCE_ENGINEER;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA GOLD TO ROLE SNOWCOMMERCE_ENGINEER;

-- ANALYST: Read access (query business-ready data)
GRANT USAGE ON SCHEMA GOLD TO ROLE SNOWCOMMERCE_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA GOLD TO ROLE SNOWCOMMERCE_ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA GOLD TO ROLE SNOWCOMMERCE_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA GOLD TO ROLE SNOWCOMMERCE_ANALYST;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA GOLD TO ROLE SNOWCOMMERCE_ANALYST;

-- VIEWER: Read access to business reports only
GRANT USAGE ON SCHEMA GOLD TO ROLE SNOWCOMMERCE_VIEWER;
GRANT SELECT ON ALL TABLES IN SCHEMA GOLD TO ROLE SNOWCOMMERCE_VIEWER;
GRANT SELECT ON ALL VIEWS IN SCHEMA GOLD TO ROLE SNOWCOMMERCE_VIEWER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA GOLD TO ROLE SNOWCOMMERCE_VIEWER;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA GOLD TO ROLE SNOWCOMMERCE_VIEWER;

-- ============================================================================
-- Verify schema grants
-- ============================================================================
SHOW GRANTS ON SCHEMA RAW;
SHOW GRANTS ON SCHEMA SILVER;
SHOW GRANTS ON SCHEMA GOLD;

-- ============================================================================
-- Verify role grants
-- ============================================================================
SHOW GRANTS TO ROLE SNOWCOMMERCE_ENGINEER;
SHOW GRANTS TO ROLE SNOWCOMMERCE_ANALYST;
SHOW GRANTS TO ROLE SNOWCOMMERCE_VIEWER;

-- ============================================================================
-- NEXT STEPS:
-- Run 04_grant_object_permissions.sql to grant table/view-level access
-- (Optional - future grants should handle this automatically)
-- ============================================================================
