-- ============================================================================
-- Snowflake RBAC Setup: Grant Database-Level Permissions
-- ============================================================================
-- This script grants database-level privileges to custom roles
-- Run this script as SYSADMIN or a role with MANAGE GRANTS privilege
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE SALES_DATABASE_MEAGAN;

-- ============================================================================
-- WAREHOUSE ACCESS - Allow roles to use compute resources
-- ============================================================================

-- Grant warehouse usage to all roles (they need compute to query)
GRANT USAGE ON WAREHOUSE SALES_LOAD TO ROLE SNOWCOMMERCE_ADMIN;
GRANT USAGE ON WAREHOUSE SALES_LOAD TO ROLE SNOWCOMMERCE_ENGINEER;
GRANT USAGE ON WAREHOUSE SALES_LOAD TO ROLE SNOWCOMMERCE_ANALYST;
GRANT USAGE ON WAREHOUSE SALES_LOAD TO ROLE SNOWCOMMERCE_VIEWER;

-- Optionally grant OPERATE privilege to engineers and admins (can suspend/resume warehouse)
GRANT OPERATE ON WAREHOUSE SALES_LOAD TO ROLE SNOWCOMMERCE_ADMIN;
GRANT OPERATE ON WAREHOUSE SALES_LOAD TO ROLE SNOWCOMMERCE_ENGINEER;

-- ============================================================================
-- DATABASE ACCESS - Grant database-level privileges
-- ============================================================================

-- ADMIN: Full control over database
GRANT ALL PRIVILEGES ON DATABASE SALES_DATABASE_MEAGAN TO ROLE SNOWCOMMERCE_ADMIN;

-- ENGINEER: Can create schemas and manage objects (needed for dbt)
GRANT USAGE ON DATABASE SALES_DATABASE_MEAGAN TO ROLE SNOWCOMMERCE_ENGINEER;
GRANT CREATE SCHEMA ON DATABASE SALES_DATABASE_MEAGAN TO ROLE SNOWCOMMERCE_ENGINEER;

-- ANALYST: Can use database (query data)
GRANT USAGE ON DATABASE SALES_DATABASE_MEAGAN TO ROLE SNOWCOMMERCE_ANALYST;

-- VIEWER: Can use database (query data)
GRANT USAGE ON DATABASE SALES_DATABASE_MEAGAN TO ROLE SNOWCOMMERCE_VIEWER;

-- ============================================================================
-- FUTURE GRANTS - Automatically grant privileges on new schemas
-- ============================================================================
-- These grants ensure that when new schemas are created, roles automatically
-- get appropriate permissions without manual intervention

-- Admin gets all privileges on future schemas
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE SALES_DATABASE_MEAGAN TO ROLE SNOWCOMMERCE_ADMIN;

-- Engineer gets usage and create table on future schemas (needed for dbt builds)
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE SALES_DATABASE_MEAGAN TO ROLE SNOWCOMMERCE_ENGINEER;
GRANT CREATE TABLE ON FUTURE SCHEMAS IN DATABASE SALES_DATABASE_MEAGAN TO ROLE SNOWCOMMERCE_ENGINEER;
GRANT CREATE VIEW ON FUTURE SCHEMAS IN DATABASE SALES_DATABASE_MEAGAN TO ROLE SNOWCOMMERCE_ENGINEER;

-- ============================================================================
-- Verify grants
-- ============================================================================
SHOW GRANTS TO ROLE SNOWCOMMERCE_ADMIN;
SHOW GRANTS TO ROLE SNOWCOMMERCE_ENGINEER;
SHOW GRANTS TO ROLE SNOWCOMMERCE_ANALYST;
SHOW GRANTS TO ROLE SNOWCOMMERCE_VIEWER;

-- ============================================================================
-- NEXT STEPS:
-- Run 03_grant_schema_permissions.sql to grant schema-level access
-- ============================================================================
