-- ============================================================================
-- Snowflake RBAC Setup: Assign Roles to Users
-- ============================================================================
-- This script assigns custom roles to users based on their job function.
-- Run this script as SECURITYADMIN or a role with MANAGE GRANTS privilege.
--
-- IMPORTANT: Replace example email addresses with actual team member emails
-- ============================================================================

USE ROLE SECURITYADMIN;

-- ============================================================================
-- ASSIGN ADMIN ROLE
-- ============================================================================
-- Admins have full control over the snowcommerce_analytics project
-- Typically: Project leads, senior data engineers

-- Example: Assign admin role to yourself
GRANT ROLE SNOWCOMMERCE_ADMIN TO USER "MEAGAN.AHMED@SLALOM.COM";

-- Example: Assign admin role to other team leads
-- GRANT ROLE SNOWCOMMERCE_ADMIN TO USER "JOHN.DOE@SLALOM.COM";

-- ============================================================================
-- ASSIGN ENGINEER ROLE
-- ============================================================================
-- Engineers can read/write to all layers (bronze, silver, gold)
-- Typically: Data engineers, analytics engineers who build dbt pipelines

-- Example: Assign engineer role to data engineers
-- GRANT ROLE SNOWCOMMERCE_ENGINEER TO USER "JANE.SMITH@SLALOM.COM";
-- GRANT ROLE SNOWCOMMERCE_ENGINEER TO USER "BOB.JOHNSON@SLALOM.COM";

-- ============================================================================
-- ASSIGN ANALYST ROLE
-- ============================================================================
-- Analysts have read access to silver and gold layers
-- Typically: Data analysts, business analysts who query data

-- Example: Assign analyst role to data analysts
-- GRANT ROLE SNOWCOMMERCE_ANALYST TO USER "ALICE.WILLIAMS@SLALOM.COM";
-- GRANT ROLE SNOWCOMMERCE_ANALYST TO USER "CHARLIE.BROWN@SLALOM.COM";

-- ============================================================================
-- ASSIGN VIEWER ROLE
-- ============================================================================
-- Viewers have read-only access to gold layer (business reports only)
-- Typically: Business stakeholders, executives, product managers

-- Example: Assign viewer role to stakeholders
-- GRANT ROLE SNOWCOMMERCE_VIEWER TO USER "EVE.DAVIS@SLALOM.COM";
-- GRANT ROLE SNOWCOMMERCE_VIEWER TO USER "FRANK.MILLER@SLALOM.COM";

-- ============================================================================
-- SET DEFAULT ROLES (Optional but Recommended)
-- ============================================================================
-- Setting a default role means users automatically use this role when they log in
-- This prevents confusion and ensures consistent access patterns

-- Example: Set default role for yourself
-- ALTER USER "MEAGAN.AHMED@SLALOM.COM" SET DEFAULT_ROLE = SNOWCOMMERCE_ENGINEER;

-- Example: Set default roles for team members
-- ALTER USER "JANE.SMITH@SLALOM.COM" SET DEFAULT_ROLE = SNOWCOMMERCE_ENGINEER;
-- ALTER USER "ALICE.WILLIAMS@SLALOM.COM" SET DEFAULT_ROLE = SNOWCOMMERCE_ANALYST;
-- ALTER USER "EVE.DAVIS@SLALOM.COM" SET DEFAULT_ROLE = SNOWCOMMERCE_VIEWER;

-- ============================================================================
-- Verify role assignments
-- ============================================================================
-- Check what roles a specific user has
SHOW GRANTS TO USER "MEAGAN.AHMED@SLALOM.COM";

-- Check all users who have a specific role
SHOW GRANTS OF ROLE SNOWCOMMERCE_ADMIN;
SHOW GRANTS OF ROLE SNOWCOMMERCE_ENGINEER;
SHOW GRANTS OF ROLE SNOWCOMMERCE_ANALYST;
SHOW GRANTS OF ROLE SNOWCOMMERCE_VIEWER;

-- ============================================================================
-- TESTING ACCESS (Run as different roles)
-- ============================================================================
-- After assigning roles, test that access works as expected

-- Test as Engineer (should work)
USE ROLE SNOWCOMMERCE_ENGINEER;
SELECT * FROM SALES_DATABASE_MEAGAN.RAW.CUSTOMERS LIMIT 5;
SELECT * FROM SALES_DATABASE_MEAGAN.SILVER.STG_CUSTOMERS LIMIT 5;
SELECT * FROM SALES_DATABASE_MEAGAN.GOLD.DIM_CUSTOMERS LIMIT 5;

-- Test as Analyst (should work for silver/gold, fail for raw)
USE ROLE SNOWCOMMERCE_ANALYST;
-- SELECT * FROM SALES_DATABASE_MEAGAN.RAW.CUSTOMERS LIMIT 5;  -- Should FAIL
SELECT * FROM SALES_DATABASE_MEAGAN.SILVER.STG_CUSTOMERS LIMIT 5;  -- Should WORK
SELECT * FROM SALES_DATABASE_MEAGAN.GOLD.DIM_CUSTOMERS LIMIT 5;    -- Should WORK

-- Test as Viewer (should only work for gold)
USE ROLE SNOWCOMMERCE_VIEWER;
-- SELECT * FROM SALES_DATABASE_MEAGAN.RAW.CUSTOMERS LIMIT 5;       -- Should FAIL
-- SELECT * FROM SALES_DATABASE_MEAGAN.SILVER.STG_CUSTOMERS LIMIT 5; -- Should FAIL
SELECT * FROM SALES_DATABASE_MEAGAN.GOLD.DIM_CUSTOMERS LIMIT 5;      -- Should WORK

-- ============================================================================
-- REVOKE ROLES (If needed)
-- ============================================================================
-- If you need to remove a role from a user, use REVOKE

-- Example: Remove engineer role from a user
-- REVOKE ROLE SNOWCOMMERCE_ENGINEER FROM USER "BOB.JOHNSON@SLALOM.COM";

-- Example: Remove all snowcommerce roles from a user
-- REVOKE ROLE SNOWCOMMERCE_ADMIN FROM USER "JOHN.DOE@SLALOM.COM";
-- REVOKE ROLE SNOWCOMMERCE_ENGINEER FROM USER "JOHN.DOE@SLALOM.COM";
-- REVOKE ROLE SNOWCOMMERCE_ANALYST FROM USER "JOHN.DOE@SLALOM.COM";
-- REVOKE ROLE SNOWCOMMERCE_VIEWER FROM USER "JOHN.DOE@SLALOM.COM";

-- ============================================================================
-- SETUP COMPLETE!
-- ============================================================================
-- Your RBAC setup is now complete. Users can now:
-- - Log in to Snowflake
-- - Use their assigned roles (or USE ROLE to switch)
-- - Access data according to their permission level
--
-- Recommended next steps:
-- 1. Document role assignments in your team wiki/confluence
-- 2. Create a runbook for onboarding new team members
-- 3. Review role assignments quarterly
-- 4. Monitor query history to ensure roles are being used correctly
-- ============================================================================
