# Snowflake RBAC Setup for snowcommerce_analytics

This directory contains SQL scripts to set up Role-Based Access Control (RBAC) for the `snowcommerce_analytics` dbt project in Snowflake, following Medallion Architecture principles.

## 📋 Table of Contents

- [Overview](#overview)
- [Role Hierarchy](#role-hierarchy)
- [Access Matrix](#access-matrix)
- [Setup Instructions](#setup-instructions)
- [Testing Access](#testing-access)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

---

## 🎯 Overview

This RBAC setup implements a least-privilege access model that aligns with the **Medallion Architecture** (Bronze → Silver → Gold):

- **Bronze Layer (RAW schema)**: Raw data sources, restricted to engineers only
- **Silver Layer (SILVER schema)**: Cleaned staging models, accessible to engineers and analysts
- **Gold Layer (GOLD schema)**: Business-ready reports, accessible to all roles including viewers

### Goals

✅ **Security**: Prevent unauthorized access to sensitive data  
✅ **Separation of Concerns**: Different roles for different job functions  
✅ **Maintainability**: Future grants ensure new objects inherit permissions  
✅ **Auditability**: Clear role hierarchy and permission tracking  

---

## 👥 Role Hierarchy

```
ACCOUNTADMIN
└── SYSADMIN
    └── SNOWCOMMERCE_ADMIN (Project Administrator)
        ├── SNOWCOMMERCE_ENGINEER (Data Engineers)
        ├── SNOWCOMMERCE_ANALYST (Data Analysts)
        └── SNOWCOMMERCE_VIEWER (Business Stakeholders)
```

### Role Descriptions

| Role | Purpose | Typical Users |
|------|---------|---------------|
| **SNOWCOMMERCE_ADMIN** | Full control over snowcommerce_analytics project | Project leads, senior data engineers |
| **SNOWCOMMERCE_ENGINEER** | Build and maintain dbt pipelines (read/write all layers) | Data engineers, analytics engineers |
| **SNOWCOMMERCE_ANALYST** | Query cleaned data for analysis (read silver/gold) | Data analysts, business analysts |
| **SNOWCOMMERCE_VIEWER** | View business reports only (read gold) | Business stakeholders, executives, PMs |

---

## 🔐 Access Matrix

### Layer Access by Role

| Layer | Schema | ADMIN | ENGINEER | ANALYST | VIEWER |
|-------|--------|-------|----------|---------|--------|
| **Bronze** (Raw sources) | `RAW` | ✅ Full | ✅ Read/Write | ❌ No Access | ❌ No Access |
| **Silver** (Staging) | `SILVER` | ✅ Full | ✅ Read/Write | ✅ Read | ❌ No Access |
| **Gold** (Analytics) | `GOLD` | ✅ Full | ✅ Read/Write | ✅ Read | ✅ Read |

### Detailed Permissions

#### SNOWCOMMERCE_ADMIN
- ✅ All privileges on database, schemas, tables, views
- ✅ Can create/drop schemas
- ✅ Can grant/revoke permissions
- ✅ Can operate warehouse (suspend/resume)

#### SNOWCOMMERCE_ENGINEER
- ✅ USAGE on database and warehouse
- ✅ CREATE TABLE, CREATE VIEW on all schemas
- ✅ SELECT, INSERT, UPDATE, DELETE on RAW, SILVER, GOLD
- ✅ Can run `dbt seed`, `dbt run`, `dbt test`
- ✅ Can build incremental models and snapshots

#### SNOWCOMMERCE_ANALYST
- ✅ USAGE on database and warehouse
- ✅ SELECT on SILVER and GOLD schemas
- ❌ Cannot access RAW layer
- ❌ Cannot create or modify objects

#### SNOWCOMMERCE_VIEWER
- ✅ USAGE on database and warehouse
- ✅ SELECT on GOLD schema only
- ❌ Cannot access RAW or SILVER layers
- ❌ Cannot create or modify objects

---

## 🚀 Setup Instructions

### Prerequisites

- Snowflake account access
- `SYSADMIN` or `ACCOUNTADMIN` role
- Existing database: `SALES_DATABASE_MEAGAN`
- Existing warehouse: `SALES_LOAD`

### Execution Order

Run the SQL scripts in the following order:

#### 1️⃣ Create Roles
```sql
-- Run as SYSADMIN
-- File: 01_create_roles.sql
-- Creates 4 custom roles with proper hierarchy
```

**What it does:**
- Creates `SNOWCOMMERCE_ADMIN`, `SNOWCOMMERCE_ENGINEER`, `SNOWCOMMERCE_ANALYST`, `SNOWCOMMERCE_VIEWER`
- Sets up role inheritance (admin → engineer, admin → analyst → viewer)

#### 2️⃣ Grant Database Permissions
```sql
-- Run as SYSADMIN
-- File: 02_grant_database_permissions.sql
-- Grants database and warehouse usage
```

**What it does:**
- Grants warehouse usage to all roles
- Grants database-level privileges
- Sets up future grants for new schemas

#### 3️⃣ Grant Schema Permissions
```sql
-- Run as SYSADMIN
-- File: 03_grant_schema_permissions.sql
-- Grants schema-level access for RAW, SILVER, GOLD
```

**What it does:**
- Creates SILVER and GOLD schemas if they don't exist
- Grants appropriate access to each layer per role
- Sets up future grants for new tables/views

#### 4️⃣ Grant Object Permissions (Optional)
```sql
-- Run as SYSADMIN
-- File: 04_grant_object_permissions.sql
-- Backfills permissions on existing tables/views
```

**What it does:**
- Grants SELECT on existing tables and views
- **Note**: May not be needed if future grants are working

#### 5️⃣ Assign Roles to Users
```sql
-- Run as SECURITYADMIN
-- File: 05_assign_roles_to_users.sql
-- Assigns roles to team members
```

**What it does:**
- Grants roles to specific users (replace example emails)
- Sets default roles for users
- Includes test queries to verify access

---

## 🧪 Testing Access

After setup, test that roles work as expected:

### Test as Engineer
```sql
USE ROLE SNOWCOMMERCE_ENGINEER;
USE WAREHOUSE SALES_LOAD;

-- Should all work:
SELECT * FROM SALES_DATABASE_MEAGAN.RAW.CUSTOMERS LIMIT 5;
SELECT * FROM SALES_DATABASE_MEAGAN.SILVER.STG_CUSTOMERS LIMIT 5;
SELECT * FROM SALES_DATABASE_MEAGAN.GOLD.DIM_CUSTOMERS LIMIT 5;
```

### Test as Analyst
```sql
USE ROLE SNOWCOMMERCE_ANALYST;
USE WAREHOUSE SALES_LOAD;

-- Should FAIL (no access to raw):
SELECT * FROM SALES_DATABASE_MEAGAN.RAW.CUSTOMERS LIMIT 5;

-- Should WORK (access to silver and gold):
SELECT * FROM SALES_DATABASE_MEAGAN.SILVER.STG_CUSTOMERS LIMIT 5;
SELECT * FROM SALES_DATABASE_MEAGAN.GOLD.DIM_CUSTOMERS LIMIT 5;
```

### Test as Viewer
```sql
USE ROLE SNOWCOMMERCE_VIEWER;
USE WAREHOUSE SALES_LOAD;

-- Should FAIL (no access to raw or silver):
SELECT * FROM SALES_DATABASE_MEAGAN.RAW.CUSTOMERS LIMIT 5;
SELECT * FROM SALES_DATABASE_MEAGAN.SILVER.STG_CUSTOMERS LIMIT 5;

-- Should WORK (access to gold):
SELECT * FROM SALES_DATABASE_MEAGAN.GOLD.DIM_CUSTOMERS LIMIT 5;
SELECT * FROM SALES_DATABASE_MEAGAN.GOLD.RPT_CUSTOMER_METRICS LIMIT 5;
```

---

## ✅ Best Practices

### 1. Use Least Privilege Principle
- Grant only the minimum permissions needed for each role
- Don't grant admin rights unless absolutely necessary
- Regularly audit role assignments

### 2. Leverage Role Hierarchy
- Use role inheritance to simplify permission management
- Grant engineer role to admin, analyst role to engineer, etc.
- Avoid granting roles directly from ACCOUNTADMIN

### 3. Future Grants Are Your Friend
```sql
-- Always set up future grants when creating schemas:
GRANT SELECT ON FUTURE TABLES IN SCHEMA GOLD TO ROLE SNOWCOMMERCE_VIEWER;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA GOLD TO ROLE SNOWCOMMERCE_VIEWER;
```

### 4. Set Default Roles
```sql
-- Makes user experience smoother:
ALTER USER "USER@EMAIL.COM" SET DEFAULT_ROLE = SNOWCOMMERCE_ANALYST;
```

### 5. Use Secure Views for Row-Level Security
```sql
-- If you need to restrict access to specific rows:
CREATE OR REPLACE SECURE VIEW GOLD.RPT_REGIONAL_SALES AS
SELECT *
FROM GOLD.RPT_SALES
WHERE region = CURRENT_USER_REGION();  -- Custom function
```

### 6. Document Everything
- Keep this README updated
- Document role assignments in your team wiki
- Create runbooks for onboarding new team members

### 7. Monitor and Audit
```sql
-- Regularly check role assignments:
SHOW GRANTS TO ROLE SNOWCOMMERCE_ENGINEER;
SHOW GRANTS OF ROLE SNOWCOMMERCE_ENGINEER;

-- Check who's using what:
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE USER_NAME = 'SPECIFIC.USER@EMAIL.COM'
ORDER BY START_TIME DESC
LIMIT 100;
```

---

## 🔧 Troubleshooting

### Issue: User can't access tables/views

**Problem**: User gets "Insufficient privileges" error

**Solutions**:
1. Verify user has the role assigned:
   ```sql
   SHOW GRANTS TO USER "USER@EMAIL.COM";
   ```

2. Verify role has the right permissions:
   ```sql
   SHOW GRANTS TO ROLE SNOWCOMMERCE_ANALYST;
   ```

3. Check if future grants are set up:
   ```sql
   SHOW FUTURE GRANTS IN SCHEMA GOLD;
   ```

4. Manually grant access to existing objects:
   ```sql
   GRANT SELECT ON ALL TABLES IN SCHEMA GOLD TO ROLE SNOWCOMMERCE_ANALYST;
   GRANT SELECT ON ALL VIEWS IN SCHEMA GOLD TO ROLE SNOWCOMMERCE_ANALYST;
   ```

### Issue: dbt can't create objects

**Problem**: `dbt run` fails with "Insufficient privileges to create table"

**Solutions**:
1. Ensure dbt is using the ENGINEER role:
   ```yaml
   # profiles.yml
   role: SNOWCOMMERCE_ENGINEER  # or PUBLIC if it has sufficient grants
   ```

2. Verify engineer role has CREATE privileges:
   ```sql
   SHOW GRANTS TO ROLE SNOWCOMMERCE_ENGINEER;
   ```

3. Grant CREATE TABLE/VIEW explicitly:
   ```sql
   GRANT CREATE TABLE ON SCHEMA GOLD TO ROLE SNOWCOMMERCE_ENGINEER;
   GRANT CREATE VIEW ON SCHEMA SILVER TO ROLE SNOWCOMMERCE_ENGINEER;
   ```

### Issue: Role hierarchy not working

**Problem**: Admin can't switch to engineer role

**Solutions**:
1. Check role grants:
   ```sql
   SHOW GRANTS TO ROLE SNOWCOMMERCE_ADMIN;
   ```

2. Grant child roles to parent:
   ```sql
   GRANT ROLE SNOWCOMMERCE_ENGINEER TO ROLE SNOWCOMMERCE_ADMIN;
   ```

### Issue: New tables don't inherit permissions

**Problem**: Viewer can't access newly created reports

**Solutions**:
1. Check future grants:
   ```sql
   SHOW FUTURE GRANTS IN SCHEMA GOLD;
   ```

2. Set up future grants:
   ```sql
   GRANT SELECT ON FUTURE TABLES IN SCHEMA GOLD TO ROLE SNOWCOMMERCE_VIEWER;
   ```

3. Backfill existing tables:
   ```sql
   GRANT SELECT ON ALL TABLES IN SCHEMA GOLD TO ROLE SNOWCOMMERCE_VIEWER;
   ```

---

## 📚 Additional Resources

- [Snowflake Access Control Overview](https://docs.snowflake.com/en/user-guide/security-access-control-overview.html)
- [Snowflake Role-Based Access Control](https://docs.snowflake.com/en/user-guide/security-access-control-considerations.html)
- [dbt + Snowflake Permissions](https://docs.getdbt.com/reference/warehouse-setups/snowflake-setup#required-permissions)
- [Medallion Architecture Best Practices](https://www.databricks.com/glossary/medallion-architecture)

---

## 📝 Maintenance Log

| Date | Change | By |
|------|--------|-----|
| 2026-01-15 | Initial RBAC setup | Meagan Ahmed |
| | | |

---

## 🤝 Contributing

To request access or report permission issues:
1. Contact the project admin (Meagan Ahmed)
2. Specify which role you need and why
3. Admin will run the appropriate grant statements

---

**Setup Version**: 1.0  
**Last Updated**: January 15, 2026  
**Maintained By**: Meagan Ahmed (meagan.ahmed@slalom.com)
