-- ============================================================================
-- SNOWFLAKE RBAC QUICK REFERENCE
-- ============================================================================
-- Copy-paste commands for common RBAC operations
-- ============================================================================

-- ============================================================================
-- VIEW CURRENT ROLE & SWITCH ROLES
-- ============================================================================

-- Check current role
SELECT CURRENT_ROLE();

-- Switch to different role
USE ROLE SNOWCOMMERCE_ADMIN;
USE ROLE SNOWCOMMERCE_ENGINEER;
USE ROLE SNOWCOMMERCE_ANALYST;
USE ROLE SNOWCOMMERCE_VIEWER;

-- ============================================================================
-- CHECK ROLE ASSIGNMENTS
-- ============================================================================

-- See all roles you have access to
SHOW GRANTS TO USER CURRENT_USER();

-- See what a specific role can do
SHOW GRANTS TO ROLE SNOWCOMMERCE_ENGINEER;

-- See who has a specific role
SHOW GRANTS OF ROLE SNOWCOMMERCE_ANALYST;

-- ============================================================================
-- VERIFY ACCESS TO LAYERS
-- ============================================================================

-- Test Bronze layer (RAW) access
USE ROLE SNOWCOMMERCE_ENGINEER;
SELECT COUNT(*) FROM SALES_DATABASE_MEAGAN.RAW.CUSTOMERS;

-- Test Silver layer access
USE ROLE SNOWCOMMERCE_ANALYST;
SELECT COUNT(*) FROM SALES_DATABASE_MEAGAN.SILVER.STG_CUSTOMERS;

-- Test Gold layer access
USE ROLE SNOWCOMMERCE_VIEWER;
SELECT COUNT(*) FROM SALES_DATABASE_MEAGAN.GOLD.DIM_CUSTOMERS;

-- ============================================================================
-- GRANT ROLE TO NEW USER (Run as SECURITYADMIN)
-- ============================================================================

USE ROLE SECURITYADMIN;

-- Grant engineer role
GRANT ROLE SNOWCOMMERCE_ENGINEER TO USER "NEW.USER@SLALOM.COM";

-- Set default role
ALTER USER "NEW.USER@SLALOM.COM" SET DEFAULT_ROLE = SNOWCOMMERCE_ENGINEER;

-- ============================================================================
-- GRANT ACCESS TO NEW SCHEMA (Run as SYSADMIN)
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE SALES_DATABASE_MEAGAN;

-- Grant schema usage
GRANT USAGE ON SCHEMA NEW_SCHEMA TO ROLE SNOWCOMMERCE_ANALYST;

-- Grant select on all existing tables
GRANT SELECT ON ALL TABLES IN SCHEMA NEW_SCHEMA TO ROLE SNOWCOMMERCE_ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA NEW_SCHEMA TO ROLE SNOWCOMMERCE_ANALYST;

-- Grant select on future tables (auto-inherit)
GRANT SELECT ON FUTURE TABLES IN SCHEMA NEW_SCHEMA TO ROLE SNOWCOMMERCE_ANALYST;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA NEW_SCHEMA TO ROLE SNOWCOMMERCE_ANALYST;

-- ============================================================================
-- AUDIT QUERIES
-- ============================================================================

-- Who ran queries in the last 24 hours?
SELECT
    USER_NAME,
    ROLE_NAME,
    QUERY_TYPE,
    DATABASE_NAME,
    SCHEMA_NAME,
    START_TIME,
    EXECUTION_STATUS,
    QUERY_TEXT
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD(day, -1, CURRENT_TIMESTAMP())
    AND DATABASE_NAME = 'SALES_DATABASE_MEAGAN'
ORDER BY START_TIME DESC
LIMIT 100;

-- What roles does each user have?
SELECT
    GRANTEE_NAME AS USER_NAME,
    ROLE AS ROLE_NAME,
    GRANTED_ON,
    GRANTED_BY
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS
WHERE DELETED_ON IS NULL
    AND ROLE LIKE 'SNOWCOMMERCE_%'
ORDER BY USER_NAME, ROLE_NAME;

-- What permissions does a role have?
SELECT
    PRIVILEGE,
    GRANTED_ON,
    NAME AS OBJECT_NAME,
    TABLE_CATALOG AS DATABASE_NAME,
    TABLE_SCHEMA AS SCHEMA_NAME
FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE GRANTEE_NAME = 'SNOWCOMMERCE_ANALYST'
    AND DELETED_ON IS NULL
ORDER BY GRANTED_ON, NAME;

-- ============================================================================
-- TROUBLESHOOTING
-- ============================================================================

-- Fix: User can't see new tables
USE ROLE SYSADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA GOLD TO ROLE SNOWCOMMERCE_VIEWER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA GOLD TO ROLE SNOWCOMMERCE_VIEWER;

-- Fix: dbt can't create tables
USE ROLE SYSADMIN;
GRANT CREATE TABLE ON SCHEMA GOLD TO ROLE SNOWCOMMERCE_ENGINEER;
GRANT CREATE VIEW ON SCHEMA SILVER TO ROLE SNOWCOMMERCE_ENGINEER;

-- Fix: Role hierarchy broken
USE ROLE SECURITYADMIN;
GRANT ROLE SNOWCOMMERCE_ENGINEER TO ROLE SNOWCOMMERCE_ADMIN;
GRANT ROLE SNOWCOMMERCE_ANALYST TO ROLE SNOWCOMMERCE_ADMIN;

-- ============================================================================
-- REVOKE ACCESS (If needed)
-- ============================================================================

USE ROLE SECURITYADMIN;

-- Revoke role from user
REVOKE ROLE SNOWCOMMERCE_ANALYST FROM USER "USER@EMAIL.COM";

-- Revoke schema access from role
USE ROLE SYSADMIN;
REVOKE USAGE ON SCHEMA GOLD FROM ROLE SNOWCOMMERCE_VIEWER;
