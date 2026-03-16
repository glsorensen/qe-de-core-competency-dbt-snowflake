# Architecture Documentation

**dbt + Snowflake Medallion Architecture**

A comprehensive guide to the system design, data flow, technology stack, and technical decisions for the snowcommerce_analytics project.

---

## 📋 Table of Contents

1. [System Overview](#system-overview)
2. [Medallion Architecture Layers](#medallion-architecture-layers)
3. [Data Flow](#data-flow)
4. [Technology Stack](#technology-stack)
5. [Schema Design](#schema-design)
6. [Data Models](#data-models)
7. [Integration Patterns](#integration-patterns)
8. [Security & Governance](#security--governance)
9. [Scalability & Performance](#scalability--performance)
10. [Development & CI/CD](#development--cicd)

---

## System Overview

### Purpose

This project demonstrates an **enterprise-grade data transformation pipeline** using:
- **dbt** (data build tool) for SQL transformations and testing
- **Snowflake** for cloud data warehousing
- **Medallion Architecture** for layered data organization
- **S3 Integration** for external data sourcing

### Key Characteristics

- **Source-agnostic**: Data starts from S3 via Snowflake storage integration
- **Transformation-focused**: Multi-layer transformations (raw → cleaned → business-ready)
- **Quality-driven**: 225+ automated tests across all layers
- **Audit-enabled**: Query tagging and execution logging
- **Documentation-automated**: Auto-generated dbt docs with lineage

### Goals

1. ✅ Demonstrate industry-standard data layering patterns
2. ✅ Implement comprehensive data quality testing
3. ✅ Enable audit trail tracking and cost monitoring
4. ✅ Create reusable, maintainable data transformation code
5. ✅ Provide learning path for dbt/Snowflake beginners

---

## Medallion Architecture Layers

### Overview

The Medallion Architecture uses three layers, each with distinct purposes, materializations, and transformations:

```
┌─────────────────────────────────────────────────────────────┐
│          DATA TRANSFORMATION LIFECYCLE                       │
│          (Bronze → Silver → Gold)                            │
└─────────────────────────────────────────────────────────────┘

EXTERNAL         SNOWFLAKE        dbt MODELS           PURPOSE
DATA SOURCE      RAW TABLES       & TRANSFORMATIONS    
═══════════════════════════════════════════════════════════════

   S3 Bucket        RAW Schema      BRONZE LAYER        Raw Data
   (CSVs)      ────────────────────[sources.yml]───➜   No Changes
                                                       
                ────────────────────[SILVER VIEWS]───➜ Clean Data
                                   (stg_*)            Standardized
                
                ────────────────────[GOLD TABLES]────➜ Business Data
                                   (dim_*, fct_*,    Ready for BI
                                    rpt_*)
```

### Layer 1: Bronze (Raw)

**Purpose:** Exact replica of raw source data

**Schema:** `RAW`

**Materialization:** `EXTERNAL TABLE` (pointing to S3)

**Characteristics:**
- No transformations applied
- 1:1 mapping with source files
- Earliest possible stage for testing
- Immutable snapshot of source

**Tables:**
- `RAW.CUSTOMERS` - Raw customer data (10 cols)
- `RAW.ORDERS` - Raw order transactions (5 cols)
- `RAW.PRODUCTS` - Raw product catalog (5 cols)

**Data Quality:**
- Null checks on primary keys
- Uniqueness on identifiers
- Relationship validation

**Example - Customers:**
```
Column              Type          Source       Transform
─────────────────────────────────────────────────────────
customer_id         INTEGER       ✓ Direct     None
first_name          VARCHAR       ✓ Direct     None
last_name           VARCHAR       ✓ Direct     None
email               VARCHAR       ✓ Direct     None
phone               VARCHAR       ✓ Direct     None
address             VARCHAR       ✓ Direct     None
city                VARCHAR       ✓ Direct     None
state               VARCHAR       ✓ Direct     None
zip_code            VARCHAR       ✓ Direct     None
created_at          TIMESTAMP     ✓ Direct     None
```

### Layer 2: Silver (Cleaned/Staging)

**Purpose:** Standardized, cleaned, and deduplicated data

**Schema:** `SILVER`

**Materialization:** `VIEW` (computed on query)

**Characteristics:**
- Light transformations applied
- Type casting and standardization
- Null handling and deduplication
- Conformed dimensions
- Reusable staging models

**Transformations Applied:**
- ✅ Type casting (strings to dates, decimals)
- ✅ Column renaming for consistency
- ✅ Null value handling
- ✅ Deduplication
- ✅ Date dimension extraction
- ✅ Load timestamp addition

**Models:**
- `SILVER.STG_CUSTOMERS` - Cleaned customers (10 cols)
- `SILVER.STG_ORDERS` - Cleaned orders with date dims (11 cols)
- `SILVER.STG_ORDER_ITEMS` - Line item staging (6 cols)
- `SILVER.STG_PRODUCTS` - Cleaned products (6 cols)

**Example - stg_orders Transformations:**
```
Bronze (RAW.ORDERS)          →  Silver (STG_ORDERS)
─────────────────                ────────────────
order_id                         order_id
customer_id                      customer_id
order_date                       order_date
order_status                     order_status (validated)
total_amount                     total_amount (cleaned)
                          +      order_year (extracted)
                          +      order_month (extracted)
                          +      order_quarter (extracted)
                          +      order_day_of_week (extracted)
                          +      created_at (load timestamp)
```

**View Strategy:**
- Views used because:
  - No storage cost in Snowflake
  - Fast iteration during development
  - Always reflects latest raw data
  - Source-of-truth for downstream
  
### Layer 3: Gold (Business-Ready)

**Purpose:** Analytics-ready, denormalized business models

**Schema:** `GOLD`

**Materialization:** `TABLE` (persisted for performance)

**Characteristics:**
- Business logic applied
- Denormalized for query performance
- Aggregations and calculations
- Role-based model organization (dimensions, facts, reports)

**Model Types:**

#### Dimension Tables (`dim_*`):
One row per business entity, with attributes and slowly-changing dimensions

**DIM_CUSTOMERS** (13 columns):
- Customer identity (id, name, email, contact)
- Customer metrics (total_orders, lifetime_revenue, avg_order_value)
- Customer segment (VIP/High-Value/Standard)
- Temporal (first_order_date, last_order_date, days_since_last_order)

**DIM_PRODUCTS** (10 columns):
- Product identity (id, name, category)
- Product metrics (average_price, total_quantity_sold)
- Performance indicators (popularity_rank, revenue_rank)

#### Fact Tables (`fct_*`):
One row per business event (transaction, order, etc.)

**FCT_ORDERS** (20 columns):
- Order identity & dates
- Customer denormalization (id, name, email, segment)
- Order metrics (total_amount, item_count, discount_total)
- Status tracking & timestamps

**FCT_ORDER_ITEMS** (10 columns):
- Item identity (id, order_id, product_id)
- Item metrics (quantity, unit_price, line_total)
- Calculated metrics (discount_pct, tax_amount)

#### Report Tables (`rpt_*`):
Pre-aggregated tables for reporting

**RPT_CUSTOMER_METRICS** (7 columns):
- Monthly customer activity
- Order count, revenue, average order value
- Pre-aggregated for dashboard queries

**RPT_MONTHLY_SALES** (8 columns):
- Monthly sales summary
- Revenue, order count, customer count
- Category breakdown

**RPT_PRODUCT_PERFORMANCE** (11 columns):
- Product-level sales metrics
- Revenue, quantity sold, average price
- Performance rankings

---

## Data Flow

### End-to-End Data Journey

```
┌─────────────────────────────────────────────────────────────────────┐
│                      DATA FLOW ARCHITECTURE                         │
└─────────────────────────────────────────────────────────────────────┘

1. DATA SOURCE STAGE
   ├── S3 Bucket (External Storage)
   │   ├── customers.csv (10 rows)
   │   ├── orders.csv (20 rows)
   │   ├── products.csv (15 rows)
   │   └── order_items.csv (50+ rows)
   │
   └── Uploaded as-is (no preprocessing)

2. INGESTION STAGE
   ├── Snowflake Storage Integration
   │   └── Connects to S3 bucket via IAM role
   │
   └── External Stage Definition
       └── Creates reference to S3 location with file format

3. BRONZE LAYER STAGE
   ├── CREATE EXTERNAL TABLE AS
   │   └── Reads from external stage
   │
   └── RAW schema tables created
       ├── RAW.CUSTOMERS
       ├── RAW.ORDERS
       └── RAW.PRODUCTS

4. SILVER LAYER STAGE (dbt run --select silver)
   ├── dbt compiles models
   │   ├── Reads from {{ source('raw', 'customers') }}
   │   ├── Applies transformations
   │   └── Outputs to SILVER schema
   │
   └── Creates VIEWs for reusability
       ├── SILVER.STG_CUSTOMERS
       ├── SILVER.STG_ORDERS
       └── SILVER.STG_PRODUCTS

5. GOLD LAYER STAGE (dbt run --select gold)
   ├── dbt compiles models
   │   ├── Reads from {{ ref('stg_customers') }}
   │   ├── Joins multiple sources
   │   ├── Applies business logic
   │   └── Outputs to GOLD schema
   │
   └── Creates TABLEs for performance
       ├── GOLD.DIM_CUSTOMERS
       ├── GOLD.FCT_ORDERS
       └── GOLD.RPT_CUSTOMER_METRICS

6. VALIDATION STAGE (dbt test)
   ├── Generic tests (from YAML)
   │   ├── not_null, unique, accepted_values
   │   └── relationships, dbt_expectations
   │
   ├── Custom tests (from /tests directory)
   │   └── Data quality checks
   │
   └── Results recorded in target/run_results.json

7. DOCUMENTATION STAGE (dbt docs generate)
   ├── Generates data dictionary
   │   ├── models, columns, descriptions
   │   ├── lineage graph
   │   └── test coverage
   │
   └── Creates interactive UI (dbt docs serve)

8. AUDIT & MONITORING
   ├── Query tags applied to every execution
   │   └── project:snowcommerce_analytics|env:dev
   │
   ├── Execution hooks capture
   │   ├── Start timestamp
   │   ├── End timestamp
   │   └── Status (success/failure)
   │
   └── Optional audit table logs all executions
```

### Query Execution Path

```
User runs: dbt build
├── dbt deps
│   └── Downloads dbt_expectations package
│
├── dbt seed (skipped - using S3)
│
├── dbt run
│   ├── Parse models in dependency order
│   ├── Bronze layer (sources - no execution)
│   │
│   ├── Silver layer (execute sequentially)
│   │   ├── stg_customers (read from RAW.CUSTOMERS)
│   │   ├── stg_orders (read from RAW.ORDERS)
│   │   └── stg_products (read from RAW.PRODUCTS)
│   │
│   └── Gold layer (execute with dependencies)
│       ├── dim_customers (read from STG_CUSTOMERS + STG_ORDERS)
│       ├── fct_orders (read from STG_ORDERS + DIM_CUSTOMERS)
│       └── rpt_customer_metrics (read from FCT_ORDERS)
│
└── dbt test (225+ tests)
    ├── Bronze tests (sources validation)
    ├── Silver tests (stg_* validation)
    └── Gold tests (dim_*, fct_*, rpt_* validation)
```

---

## Technology Stack

### Core Components

| Component | Version | Purpose |
|-----------|---------|---------|
| **dbt-core** | 1.11.2 | SQL compilation, model orchestration, testing |
| **dbt-snowflake** | 1.11.1 | Snowflake adapter for dbt |
| **Snowflake** | Current | Cloud data warehouse |
| **Python** | 3.12 | dbt runtime environment |
| **S3** | AWS service | External data storage |

### dbt Packages

| Package | Version | Purpose |
|---------|---------|---------|
| **dbt_expectations** | 0.10.1 | Advanced data quality tests (column existence, pattern matching, etc.) |

### Development Tools

| Tool | Purpose |
|------|---------|
| **Git** | Version control |
| **Virtual Environment** | Python environment isolation |
| **pip** | Python package management |

### Snowflake Features Used

| Feature | Purpose |
|---------|---------|
| **Storage Integration** | Connect to S3 without managing credentials |
| **External Stages** | Reference S3 locations in SQL |
| **External Tables** | Query S3 files as if they were tables |
| **Query Tags** | Attach metadata to queries for tracking |
| **ACCOUNT_USAGE schema** | Access query history logs |
| **Execution Hooks** | Run SQL before/after dbt operations |

---

## Schema Design

### Database Organization

```
┌─────────────────────────────────────────────────────────┐
│           CAPSTONE_FLOWERSHOP_DN_SKS                    │
│           (Snowflake Database)                          │
└─────────────────────────────────────────────────────────┘

├── 📁 RAW Schema (Bronze Layer)
│   │
│   ├── CUSTOMERS (External Table)
│   │   └── 10 rows × 10 columns
│   │
│   ├── ORDERS (External Table)
│   │   └── 20 rows × 5 columns
│   │
│   ├── PRODUCTS (External Table)
│   │   └── 15 rows × 5 columns
│   │
│   ├── ORDER_ITEMS (External Table)
│   │   └── 50+ rows × 4 columns
│
├── 📁 SILVER Schema (Silver Layer)
│   │
│   ├── STG_CUSTOMERS (View)
│   │   └── 10 rows × 10 columns
│   │
│   ├── STG_ORDERS (View)
│   │   └── 20 rows × 11 columns (with date dimensions)
│   │
│   ├── STG_ORDER_ITEMS (View)
│   │   └── 50+ rows × 6 columns
│   │
│   ├── STG_PRODUCTS (View)
│   │   └── 15 rows × 6 columns
│
└── 📁 GOLD Schema (Gold Layer - Optimized)
    │
    ├── DIM_CUSTOMERS (Table - Clustered)
    │   └── 10 rows × 13 columns
    │
    ├── DIM_PRODUCTS (Table - Clustered)
    │   └── 15 rows × 10 columns
    │
    ├── FCT_ORDERS (Table - Clustered)
    │   └── 20 rows × 20 columns
    │
    ├── FCT_ORDER_ITEMS (Table - Clustered)
    │   └── 50+ rows × 10 columns
    │
    ├── RPT_CUSTOMER_METRICS (Table)
    │   └── Aggregated: 10 customers × 12 months = 120 rows × 7 columns
    │
    ├── RPT_MONTHLY_SALES (Table)
    │   └── Aggregated: ~240 rows × 8 columns
    │
    └── RPT_PRODUCT_PERFORMANCE (Table)
        └── Aggregated: 15 products × 11 columns
```

### Key Design Decisions

1. **External Tables for Bronze**
   - ✅ No data duplication in Snowflake
   - ✅ Direct reference to S3 files
   - ✅ Lower storage costs
   - ✅ Automatic updates as S3 files change

2. **Views for Silver**
   - ✅ Zero storage overhead
   - ✅ Always reflect latest raw data
   - ✅ Fast iteration during development
   - ✅ Source of truth for downstream

3. **Tables for Gold**
   - ✅ Optimized for frequent queries
   - ✅ Better query performance
   - ✅ Supports clustering
   - ✅ Enables incremental materialization

---

## Data Models

### Entity-Relationship Diagram (Bronze Layer)

Based on the raw source data from S3 bucket:

```
┌──────────────────┐
│   CUSTOMERS      │
│  (RAW.CUSTOMERS) │
├──────────────────┤
│ PK: customer_id  │
│ - first_name     │
│ - last_name      │
│ - email          │
│ - phone          │
│ - address        │
│ - city           │
│ - state          │
│ - zip_code       │
│ - created_at     │
└────────┬─────────┘
         │ (1)
         │ customer_id
         │
         ▼ (M)
┌──────────────────────┐
│    ORDERS            │
│  (RAW.ORDERS)        │
├──────────────────────┤
│ PK: order_id         │
│ FK: customer_id ─────┼──────┐
│ - order_date         │      │
│ - order_status       │      │
│ - total_amount       │      │
└────────┬─────────────┘      │
         │ (1)                │
         │ order_id           │
         │                    │
         ▼ (M)                │
┌──────────────────────┐     │
│   ORDER_ITEMS        │     │
│ (RAW.ORDER_ITEMS)    │     │
├──────────────────────┤     │
│ PK: order_item_id    │     │
│ FK: order_id ────────┼─────┘
│ FK: product_id ──────┼──┐
│ - quantity           │  │
│ - unit_price         │  │
│ - discount_pct       │  │
│ - tax_amount         │  │
└──────────────────────┘  │
                          │
         ┌────────────────┘
         │ (1)
         │ product_id
         │
         ▼ (M)
┌──────────────────┐
│    PRODUCTS      │
│ (RAW.PRODUCTS)   │
├──────────────────┤
│ PK: product_id   │
│ - product_name   │
│ - category       │
│ - price          │
│ - stock_quantity │
└──────────────────┘
```

**Relationships:**
- **CUSTOMERS → ORDERS**: One-to-Many (1 customer has many orders)
- **ORDERS → ORDER_ITEMS**: One-to-Many (1 order has many line items)
- **PRODUCTS → ORDER_ITEMS**: One-to-Many (1 product appears in many orders)

### Model Descriptions

#### Bronze Layer Sources (Raw Data from S3)

##### Customers (RAW.CUSTOMERS)

**Source:** S3 CSV file via external stage

**Data:** Customer master data (10 rows in sample)

**Columns:**
- `CUSTOMER_ID` (PK) - Unique customer identifier
- `FIRST_NAME` - Customer first name
- `LAST_NAME` - Customer last name
- `EMAIL` - Customer email address
- `PHONE` - Customer phone number
- `ADDRESS` - Street address
- `CITY` - City name
- `STATE` - State/province
- `ZIP_CODE` - Postal code
- `CREATED_AT` - Account creation timestamp

**Tests:** PK/FK checks, null checks on key fields

---

##### Orders (RAW.ORDERS)

**Source:** S3 CSV file via external stage

**Data:** Order transactions (20 rows in sample)

**Columns:**
- `ORDER_ID` (PK) - Unique order identifier
- `CUSTOMER_ID` (FK) - Reference to CUSTOMERS
- `ORDER_DATE` - Date order was placed
- `ORDER_STATUS` - Status: pending/processing/completed
- `TOTAL_AMOUNT` - Order total amount

**Tests:** PK/FK checks, referential integrity, null checks

---

##### Products (RAW.PRODUCTS)

**Source:** S3 CSV file via external stage

**Data:** Product master data (15 rows in sample)

**Columns:**
- `PRODUCT_ID` (PK) - Unique product identifier
- `PRODUCT_NAME` - Product name
- `CATEGORY` - Product category
- `PRICE` - Product price
- `STOCK_QUANTITY` - Current stock count

**Tests:** PK checks, null checks, price validation

---

##### Order Items (RAW.ORDER_ITEMS)

**Source:** S3 CSV file via external stage

**Data:** Order line items (50+ rows in sample)

**Columns:**
- `ORDER_ITEM_ID` (PK) - Unique line item identifier
- `ORDER_ID` (FK) - Reference to ORDERS
- `PRODUCT_ID` (FK) - Reference to PRODUCTS
- `QUANTITY` - Units ordered
- `UNIT_PRICE` - Price per unit
- `DISCOUNT_PERCENT` - Discount percentage
- `TAX_AMOUNT` - Tax amount

**Tests:** PK/FK checks, referential integrity

---

#### Gold Layer Models (Business-Ready Transformations)

##### Customers (DIM_CUSTOMERS)

**Type:** Dimension table (one row per customer)

**Key columns:**
- `CUSTOMER_ID` (PK) - Unique customer identifier
- `CUSTOMER_NAME` - Concatenated first + last name
- `EMAIL` - Customer email address
- `CUSTOMER_SEGMENT` - Calculated: VIP/High-Value/Standard
- `TOTAL_ORDERS` - Count of orders
- `LIFETIME_REVENUE` - Sum of all order totals
- `AVG_ORDER_VALUE` - Average order amount
- `FIRST_ORDER_DATE` - Date of earliest order
- `LAST_ORDER_DATE` - Date of most recent order
- `DAYS_SINCE_LAST_ORDER` - Number of days
- `PHONE`, `ADDRESS`, `CITY`, `STATE` - Contact details

##### Products (DIM_PRODUCTS)

**Type:** Dimension table (one row per product)

**Key columns:**
- `PRODUCT_ID` (PK) - Unique product identifier
- `PRODUCT_NAME` - Product name
- `CATEGORY` - Product category
- `AVERAGE_PRICE` - Average selling price
- `TOTAL_QUANTITY_SOLD` - Sum of all units sold
- `TOTAL_REVENUE` - Sum of all sales
- `POPULARITY_RANK` - Rank by quantity sold
- `REVENUE_RANK` - Rank by revenue
- `LAST_UPDATE_DATE` - When record was last updated

##### Orders (FCT_ORDERS)

**Type:** Fact table (one row per order)

**Key columns:**
- `ORDER_ID` (PK) - Unique order identifier
- `CUSTOMER_ID` (FK) - Reference to DIM_CUSTOMERS
- `ORDER_DATE` - When order was placed
- `ORDER_YEAR/MONTH/QUARTER/DAY_OF_WEEK` - Date dimensions
- `CUSTOMER_NAME`, `EMAIL`, `SEGMENT` - Denormalized customer info
- `TOTAL_AMOUNT` - Order total
- `ITEM_COUNT` - Number of line items
- `DISCOUNT_TOTAL` - Total discounts
- `TAX_AMOUNT` - Total tax
- `ORDER_STATUS` - Current status (pending/processing/completed)
- `CREATED_AT`, `UPDATED_AT` - Timestamps

##### Order Items (FCT_ORDER_ITEMS)

**Type:** Fact table (one row per line item)

**Key columns:**
- `ORDER_ITEM_ID` (PK) - Unique line item identifier
- `ORDER_ID` (FK) - Reference to FCT_ORDERS
- `PRODUCT_ID` (FK) - Reference to DIM_PRODUCTS
- `QUANTITY` - Number of units ordered
- `UNIT_PRICE` - Price per unit
- `DISCOUNT_PERCENT` - Discount percentage applied
- `TAX_AMOUNT` - Tax for this item
- `LINE_TOTAL` - Calculated: quantity × unit_price

##### Customer Metrics Report (RPT_CUSTOMER_METRICS)

**Type:** Report table (aggregated by customer × month)

**Purpose:** Dashboards, executive reporting

**Key columns:**
- `CUSTOMER_ID`, `CUSTOMER_NAME`
- `METRIC_YEAR`, `METRIC_MONTH`
- `ORDER_COUNT` - Number of orders in period
- `REVENUE` - Total revenue in period
- `AVG_ORDER_VALUE` - Average order value
- `CREATED_AT` - When metric was calculated

---

## Integration Patterns

### S3 to Snowflake (Bronze Layer)

**Architecture:**
```
S3 Bucket
  ├── Stores CSV files
  ├── Organized by table (customers/, orders/, products/)
  └── Snowflake accesses via storage integration

Snowflake Storage Integration
  ├── IAM role with S3 read permissions
  ├── Trusts Snowflake account
  └── No exposed credentials (AWS best practice)

External Stage
  ├── References S3 location
  ├── Defines file format (CSV, delimiter, header)
  └── Maps S3 path to Snowflake table

External Table
  ├── Acts like regular table
  ├── Queries S3 directly (no copy)
  └── Always reflects latest S3 files
```

**Advantages:**
- ✅ No credential management in dbt profiles
- ✅ Data stays in S3 (lower storage)
- ✅ Automatic updates as files change
- ✅ Native Snowflake integration

### Model References (dbt)

**Bronze References (Sources):**
```sql
-- In models/silver/stg_customers.sql
SELECT *
FROM {{ source('raw', 'customers') }}
```

**Silver References (Models):**
```sql
-- In models/gold/dim_customers.sql
SELECT c.*, COUNT(o.order_id) as total_orders
FROM {{ ref('stg_customers') }} c
LEFT JOIN {{ ref('stg_orders') }} o
  ON c.customer_id = o.customer_id
```

**Benefits:**
- ✅ Lineage tracking
- ✅ Automatic dependency resolution
- ✅ Easy refactoring
- ✅ Testability

### Query Tagging for Audit Trails

**Configuration:**
```yaml
# In profiles.yml
query_tag: 'project:snowcommerce_analytics|env:dev'
```

**Applied to:**
- Every `dbt run` execution
- Every `dbt test` execution
- Every `dbt docs generate` execution

**Tracked in:**
- `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`
- Custom audit table (optional)

**Use cases:**
- Cost tracking by project
- Compliance audit trails
- Performance monitoring
- Usage analytics

---

## Security & Governance

### Authentication & Authorization

**Connection Method:** SSO (Single Sign-On)

```yaml
# profiles.yml configuration
authenticator: externalbrowser  # Triggers browser SSO login
account: SLALOM-SNOWFLAKE_ILABS_QECATALYST
user: YOUR.EMAIL@SLALOM.COM
role: SNOWFLAKE-ILABS-QECATALYST
```

**Advantages:**
- ✅ No password stored in profiles.yml
- ✅ Uses corporate identity provider
- ✅ MFA support via company policy
- ✅ Automatic session refresh

### Role-Based Access Control

**Snowflake Roles:**
- `SNOWFLAKE-ILABS-QECATALYST` (primary role)
  - Can create/modify schemas and objects
  - Can run queries in warehouse
  - Can read/write to all layers

**Database/Schema Permissions:**
- `RAW schema` - Read-only (external tables)
- `SILVER schema` - Read-only (views)
- `GOLD schema` - Read/write (tables)

### Data Governance

**Column-Level Testing:**
- Every column in every table has `expect_column_to_exist` test
- Prevents schema drift
- Alerts if columns are removed

**Ref & Source Validation:**
- `{{ source() }}` validates table availability
- `{{ ref() }}` validates model dependencies
- Early failure on missing objects

**Query Tagging:**
- Every query tagged with project/environment
- Cost center tracking
- Compliance audit trails

---

## Scalability & Performance

### Current Scale

| Layer | Table Type | Row Count | Column Count | Storage |
|-------|-----------|-----------|-------------|---------|
| Bronze | External | ~100 | 22 | S3 (no cost) |
| Silver | Views | ~100 | 27 | None (computed) |
| Gold | Tables | ~100 | 71 | ~1-2 MB |

### Scaling Strategies

#### For Larger Data Volumes

**1. Incremental Models (Gold Layer)**
```sql
-- Only process new/changed data
{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='fail'
) }}

SELECT *
FROM {{ ref('stg_orders') }}
{% if execute and execute_macros_namespace.get('flags').FULL_REFRESH != true %}
WHERE created_at >= (SELECT MAX(created_at) FROM {{ this }})
{% endif %}
```

**2. Table Clustering (Gold Layer)**
```yaml
# _gold_schema.yml
models:
  - name: fct_orders
    config:
      cluster_by: ['customer_id', 'order_date']
```

**3. View Optimization (Silver Layer)**
- Keep views lightweight
- Move aggregations to gold tables
- Index frequently joined columns

#### For Higher Query Frequency

**1. Auto-refresh Strategy**
```bash
# Run dbt build on schedule
# Option: dbt Cloud scheduler or external orchestrator
```

**2. Warehouse Sizing**
```yaml
# Scale warehouse for concurrent queries
warehouse: SALES_LOAD  # Current 2XS
# Consider: S (3x throughput), M (5x), L (10x)
```

**3. Query Result Caching**
- Snowflake caches results by default
- Same query within 24 hours reuses cache
- Significantly faster subsequent runs

### Performance Considerations

**View vs Table Trade-offs:**

| Aspect | View | Table |
|--------|------|-------|
| Storage | None | Disk usage |
| Query speed | Slower (compute) | Faster (retrieval) |
| Data freshness | Always current | Depends on refresh |
| Cost | Compute only | Storage + compute |
| Use case | Staging/prototyping | Production/reports |

**Recommendation:**
- Silver: Use views (fast to update, no cost)
- Gold: Use tables (queries are frequent)

---

## Development & CI/CD

### Local Development Workflow

```
1. Pull Latest Code
   git pull origin main

2. Activate Virtual Environment
   source venv/bin/activate

3. Install Dependencies
   dbt deps

4. Create Feature Branch
   git checkout -b feature/new-model

5. Develop Model
   vim models/gold/new_model.sql

6. Add Tests
   vim models/gold/_gold_schema.yml

7. Run dbt Locally
   dbt run --select +new_model
   dbt test --select new_model

8. Generate Docs
   dbt docs generate
   dbt docs serve  # http://127.0.0.1:8000

9. Commit & Push
   git add models/gold/
   git commit -m "Add new_model"
   git push origin feature/new-model

10. Create Pull Request
    - Request code review
    - CI/CD pipeline runs tests
    - Merge to main if approved
```

### File Organization

```
models/
├── bronze/
│   ├── sources.yml          # Source definitions (Bronze)
│   └── README.md
│
├── silver/
│   ├── _silver_schema.yml   # YAML tests for views
│   ├── stg_*.sql            # Staging models
│   └── README.md
│
└── gold/
    ├── _gold_schema.yml     # YAML tests for tables
    ├── dim_*.sql            # Dimension tables
    ├── fct_*.sql            # Fact tables
    ├── rpt_*.sql            # Report tables
    └── README.md
```

### Model Naming Convention

```
bronze/sources.yml          Named by source, not model
├── source('raw', 'customers')
├── source('raw', 'orders')
└── source('raw', 'products')

silver/stg_*.sql            Prefix "stg_" for staging
├── stg_customers.sql
├── stg_orders.sql
├── stg_products.sql
└── stg_order_items.sql

gold/dim_*.sql              Prefix "dim_" for dimensions
├── dim_customers.sql
├── dim_products.sql
└── (other entities)

gold/fct_*.sql              Prefix "fct_" for facts
├── fct_orders.sql
├── fct_order_items.sql
└── (other transactions)

gold/rpt_*.sql              Prefix "rpt_" for reports
├── rpt_customer_metrics.sql
├── rpt_monthly_sales.sql
└── rpt_product_performance.sql
```

### dbt Test Integration

**Test Locations:**
- Generic tests: Defined in `_gold_schema.yml`, `_silver_schema.yml`
- dbt_expectations: Column existence in YAML
- Custom tests: Individual SQL files in `/tests`

**Test Categories:**
1. **System Tests** (225+)
   - not_null, unique, accepted_values
   - relationships, dbt_expectations

2. **Business Tests**
   - Total amounts > 0
   - Customer segments in range
   - Date logic validation

3. **Data Quality Tests**
   - Freshness checks
   - Distribution validation
   - Anomaly detection

```bash
# Run test suite
dbt test                              # All tests
dbt test --select dim_customers       # Model tests
dbt test --select tag:critical        # Tagged tests
dbt test --fail-fast                  # Stop on first failure
```

---

## Deployment Architecture

### Environments

**Development (Local)**
- Personal workspace
- Branch-based development
- Local Snowflake database
- Quick iteration cycle

**Staging (Optional)**
- Shared development environment
- Integration testing
- Full dataset (or subset)
- Pre-production validation

**Production (Main)**
- Single source of truth
- Monitoring and alerting
- Audit logging enabled
- Full dataset
- Performance optimized

### Version Control

**Git Strategy:**
```
main (stable, production)
├── release/v1.0.0
└── hotfix/critical-bug

develop (integration branch)
├── feature/new-model-1
├── feature/new-model-2
└── bugfix/schema-issue

feature/your-branch
└── Pull request → code review → merge
```

### Continuous Integration

**Pre-Merge Checks:**
1. ✅ dbt parse (YAML syntax validation)
2. ✅ dbt compile (SQL syntax validation)
3. ✅ dbt run (Model execution)
4. ✅ dbt test (225+ tests)
5. ✅ dbt docs generate (Documentation)

**Post-Merge Actions:**
1. Deploy to production
2. Run full data lineage
3. Update dashboards
4. Archive previous version

---

## Monitoring & Observability

### Query Tags

Every execution includes metadata:
```
project:snowcommerce_analytics|env:dev
├── project: snowcommerce_analytics
├── env: dev
├── user: automatically captured
├── timestamp: automatically captured
└── cost: automatically tracked
```

### Execution Logging

**Built-in Hooks (in dbt_project.yml):**
- `on-run-start`: Logs execution start
- `on-run-end`: Logs execution end with status

**Optional Audit Table:**
- `RAW.DBT_EXECUTION_AUDIT`
- Records every dbt execution
- Timestamp, status, duration, user
- Used for compliance and debugging

### Query History Analysis

**Snowflake ACCOUNT_USAGE:**
```sql
SELECT
    query_id,
    query_text,
    execution_time,
    warehouse_size,
    query_tag,
    user_name,
    start_time
FROM snowflake.account_usage.query_history
WHERE query_tag LIKE '%snowcommerce_analytics%'
ORDER BY start_time DESC
LIMIT 100;
```

**Reveals:**
- Cost by project
- Query performance trends
- Usage patterns
- Optimization opportunities

---

## Key Architecture Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Data Source** | S3 + Storage Integration | No credential management, scalable, cost-effective |
| **Bronze Materialization** | External Tables | Real-time access to S3, no duplication |
| **Silver Materialization** | Views | Fast iteration, zero storage, source of truth |
| **Gold Materialization** | Tables | Query performance, report optimization |
| **Transformation Tool** | dbt | SQL-native, version control, testing, documentation |
| **Documentation** | dbt docs | Auto-generated, always in sync, interactive lineage |
| **Testing** | 225+ tests | Data quality assurance, regression detection |
| **Audit Trail** | Query tags + logging | Compliance, cost tracking, debugging |
| **Authentication** | SSO | Secure, convenient, enterprise-standard |

---

## Related Documentation

- **[README.md](README.md)** - Quick start and overview
- **[QUICKSTART.md](QUICKSTART.md)** - 5-minute setup
- **[AUDIT_TRAIL_SETUP.md](AUDIT_TRAIL_SETUP.md)** - Query tagging and monitoring

---

**Last Updated:** March 16, 2026 | **dbt**: 1.11.2 | **Snowflake**: Current
