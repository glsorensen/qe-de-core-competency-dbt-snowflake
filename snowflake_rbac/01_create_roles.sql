-- ============================================================================
-- Snowflake RBAC Setup: Create Custom Roles
-- ============================================================================
-- This script creates a hierarchy of custom roles following best practices
-- for data warehouse access control using the Medallion Architecture.
--
-- Role Hierarchy (simplified):
--   ACCOUNTADMIN
--   └── SYSADMIN
--       └── SNOWCOMMERCE_ADMIN
--           ├── SNOWCOMMERCE_ENGINEER (full access to raw, silver, gold)
--           ├── SNOWCOMMERCE_ANALYST (read access to silver, gold)
--           └── SNOWCOMMERCE_VIEWER (read access to gold only)
--
-- Run this script as ACCOUNTADMIN or SYSADMIN
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE SALES_DATABASE_MEAGAN;

-- ============================================================================
-- 1. ADMIN ROLE - Full control over the snowcommerce_analytics project
-- ============================================================================
CREATE ROLE IF NOT EXISTS SNOWCOMMERCE_ADMIN
    COMMENT = 'Admin role for snowcommerce_analytics project - full control over all schemas';

-- Grant admin role to SYSADMIN (following Snowflake hierarchy best practices)
GRANT ROLE SNOWCOMMERCE_ADMIN TO ROLE SYSADMIN;

-- ============================================================================
-- 2. ENGINEER ROLE - Data engineers who build and maintain pipelines
-- ============================================================================
CREATE ROLE IF NOT EXISTS SNOWCOMMERCE_ENGINEER
    COMMENT = 'Engineer role for snowcommerce_analytics - can read/write to all layers (bronze, silver, gold)';

-- Grant engineer role to admin (inheritance)
GRANT ROLE SNOWCOMMERCE_ENGINEER TO ROLE SNOWCOMMERCE_ADMIN;

-- ============================================================================
-- 3. ANALYST ROLE - Analysts who query cleaned data
-- ============================================================================
CREATE ROLE IF NOT EXISTS SNOWCOMMERCE_ANALYST
    COMMENT = 'Analyst role for snowcommerce_analytics - read access to silver and gold layers';

-- Grant analyst role to admin (inheritance)
GRANT ROLE SNOWCOMMERCE_ANALYST TO ROLE SNOWCOMMERCE_ADMIN;

-- ============================================================================
-- 4. VIEWER ROLE - Stakeholders who only need access to final reports
-- ============================================================================
CREATE ROLE IF NOT EXISTS SNOWCOMMERCE_VIEWER
    COMMENT = 'Viewer role for snowcommerce_analytics - read-only access to gold layer (business reports)';

-- Grant viewer role to analyst and admin (inheritance)
GRANT ROLE SNOWCOMMERCE_VIEWER TO ROLE SNOWCOMMERCE_ANALYST;
GRANT ROLE SNOWCOMMERCE_VIEWER TO ROLE SNOWCOMMERCE_ADMIN;

-- ============================================================================
-- 5. Verify roles were created
-- ============================================================================
SHOW ROLES LIKE 'SNOWCOMMERCE_%';

-- ============================================================================
-- NEXT STEPS:
-- 1. Run 02_grant_database_permissions.sql to grant database-level access
-- 2. Run 03_grant_schema_permissions.sql to grant schema-level access
-- 3. Run 04_grant_object_permissions.sql to grant table/view access
-- 4. Run 05_assign_roles_to_users.sql to assign roles to team members
-- ============================================================================
