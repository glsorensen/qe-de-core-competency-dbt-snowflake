# 🏔️ dbt + Snowflake: Complete Medallion Architecture Guide

**A production-ready tutorial project demonstrating enterprise-grade dbt with Snowflake, the Medallion Architecture pattern, comprehensive data quality testing, and audit trail tracking.**

**Version**: 1.0.0 | **dbt**: 1.11.2 | **Snowflake Adapter**: 1.11.1 | **Python**: 3.12

---

## 📚 Table of Contents

1. [What You'll Learn](#what-youll-learn)
2. [Architecture Overview](#architecture-overview)
3. [Project Structure](#project-structure)
4. [Prerequisites & Setup](#prerequisites--setup)
5. [Data Models & Schema](#data-models--schema)
6. [Testing Framework](#testing-framework)
7. [Audit Trail & Query Tagging](#audit-trail--query-tagging)
8. [Running dbt](#running-dbt)
9. [Sample Data](#sample-data)
10. [dbt Commands Reference](#dbt-commands-reference)
11. [Documentation & Observability](#documentation--observability)
12. [Best Practices](#best-practices)
13. [Troubleshooting](#troubleshooting)

---

## 📚 What You'll Learn

- dbt fundamentals: Sources, models, refs, tests, documentation
- Medallion Architecture: Bronze → Silver → Gold pattern
- Data transformations and quality testing
- Query tagging and audit trail tracking
- Snowflake integration and optimization

---

## 🏗️ Architecture Overview

### Medallion Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                  MEDALLION ARCHITECTURE                          │
│              Bronze → Silver → Gold Pattern                      │
└──────────────────────────────────────────────────────────────────┘

┌─────────────────┐     ┌─────────────────┐     ┌──────────────────┐
│    BRONZE       │────▶│    SILVER       │────▶│     GOLD         │
│ (Raw Layer)     │     │ (Cleaned Layer) │     │(Business Layer)  │
└─────────────────┘     └─────────────────┘     └──────────────────┘
│ Schema: raw     │     │ Schema: silver  │     │ Schema: gold     │
├─────────────────┤     ├─────────────────┤     ├──────────────────┤
│ NO              │     │ • Rename cols   │     │ • Dimensions     │
│ TRANSFORMATIONS │────▶│ • Type casting  │────▶│   (dim_*)        │
│ • customers     │     │ • Handle nulls  │     │ • Facts (fct_*)  │
│ • orders        │     │ • Dedup data    │     │ • Reports        │
│ • products      │     │ • stg_*         │     │   (rpt_*)        │
│                 │     │                 │     │ • Business logic │
└─────────────────┘     └─────────────────┘     └──────────────────┘
        ▲                       ▲                       ▲
        │                       │                       │
        └───── sources.yml ─────┴─ ref() models ───────┘
```

### Data Flow

```
S3 Bucket (CSV Files)
       ↓
Snowflake External Stage (S3 Integration)
       ↓
CREATE TABLE AS SELECT from external stage
       ↓
RAW.CUSTOMERS, RAW.ORDERS, RAW.PRODUCTS (bronze tables)
       ↓
dbt run (silver layer)
       ↓
Silver.STG_CUSTOMERS, STG_ORDERS, STG_PRODUCTS (cleaned views)
       ↓
dbt run (gold layer) + dbt test (225 tests)
       ↓
Gold.DIM_CUSTOMERS, FCT_ORDERS, RPT_CUSTOMER_METRICS (tables)
       ↓
dbt docs → interactive documentation
```

---

## S3 Integration Setup

**Snowflake Storage Integration:** Connected to S3 bucket via Snowflake storage integration

**Process:**
1. Created Snowflake storage integration pointing to S3 bucket
2. Configured IAM credentials for S3 access
3. Created external stage referencing the storage integration
4. Created file format for CSV files
5. Loaded CSVs into RAW schema tables from S3

---

## 📁 Project Structure

```
qe-de-core-competency-dbt-snowflake/
│
├── 📄 README.md                          # Quick start guide
├── 📄 README_DETAILED.md                 # This file
├── 📄 AUDIT_TRAIL_SETUP.md               # Query tagging & audit setup
├── 📄 QUICKSTART.md                      # 5-minute quick start
├── 📄 dbt_project.yml                    # dbt configuration & hooks
├── 📄 profiles.yml.example               # Connection template
├── 📄 packages.yml                       # dbt package dependencies
├── 📄 requirements.txt                   # Python dependencies
│
├── 📁 models/                            # dbt transformation models
│   │
│   ├── 📁 bronze/                        # Raw source definitions
│   │   ├── sources.yml                   # Source table contracts
│   │   └── README.md                     # Bronze layer docs
│   │
│   ├── 📁 silver/                        # Cleaned staging layer
│   │   ├── _silver_schema.yml            # Schema & tests (27 columns)
│   │   ├── stg_customers.sql             # Staged customers (10 cols)
│   │   ├── stg_orders.sql                # Staged orders (11 cols)
│   │   ├── stg_order_items.sql           # Staged order items (6 cols)
│   │   ├── stg_products.sql              # Staged products (6 cols)
│   │   ├── stg_marketing_campaigns.sql   # Staged campaigns (5 cols)
│   │   └── README.md                     # Silver layer docs
│   │
│   └── 📁 gold/                          # Business-ready models
│       ├── _gold_schema.yml              # Schema & tests (71 columns)
│       ├── dim_customers.sql             # Customer dimension (13 cols)
│       ├── dim_products.sql              # Product dimension (10 cols)
│       ├── fct_order_items.sql           # Order items facts (10 cols)
│       ├── fct_orders.sql                # Orders facts (20 cols)
│       ├── rpt_customer_metrics.sql      # Customer metrics report (7 cols)
│       ├── rpt_monthly_sales.sql         # Monthly sales report (8 cols)
│       ├── rpt_product_performance.sql   # Product perf report (11 cols)
│       └── README.md                     # Gold layer docs
│
├── 📁 seeds/                             # NOT USED - data from S3
│   ├── customers.csv                     # Reference only (in S3)
│   ├── orders.csv                        # Reference only (in S3)
│   ├── products.csv                      # Reference only (in S3)
│   ├── order_items.csv                   # Reference only (in S3)
│   └── marketing_campaigns.csv           # Reference only (in S3)
│
├── 📁 tests/                             # Data quality tests
│   ├── 📁 bronze/                        # Bronze layer tests
│   │   ├── customers_not_null.sql        # Null checks
│   │   ├── orders_relationships.sql      # Foreign key checks
│   │   └── products_unique.sql           # Uniqueness checks
│   │
│   ├── 📁 silver/                        # Silver layer tests
│   │   ├── stg_customers_.*              # Staging validations
│   │   └── ...
│   │
│   └── 📁 gold/                          # Gold layer tests
│       ├── dim_customers_.*              # Dimension validations
│       └── ...
│
├── 📁 macros/                            # Reusable SQL functions
│   ├── cents_to_dollars.sql              # Price formatting
│   ├── generate_schema_name.sql          # Schema naming logic
│   ├── audit_query_tag.sql               # Audit metadata macro
│   ├── set_query_tag.sql                 # Snowflake query tagging
│   └── log_model_execution_audit.sql     # Execution logging
│
├── 📁 analyses/                          # SQL analysis files
│   └── setup_audit_table.sql             # Creates audit log table
│
├── 📁 logs/                              # dbt execution logs
│   └── dbt.log                           # Latest run log
│
├── 📁 target/                            # dbt build artifacts
│   ├── compiled/                         # Compiled SQL
│   ├── manifest.json                     # Project dependency graph
│   ├── catalog.json                      # Data dictionary
│   └── run_results.json                  # Test results
│
└── 📁 dbt_packages/                      # Installed packages
    └── dbt_expectations/                 # dbt_expectations package
```

---

## Prerequisites & Setup

### System Requirements

| Component | Requirement | Notes |
|-----------|-------------|-------|
| Python | 3.11 or 3.12 | NOT 3.13+ (dbt limitation) |
| Git | Latest | For cloning repo |
| Snowflake Account | Slalom tenant | SSO enabled |
| Terminal | zsh/bash/PowerShell | Any shell works |

### Step 1: Clone Repository

```bash
# Clone the project
git clone https://github.com/glsorensen/qe-de-core-competency-dbt-snowflake.git
cd qe-de-core-competency-dbt-snowflake

# Create your learning branch
git checkout -b your-name/learning
```

### Step 2: Create Python Virtual Environment

```bash
# Use Python 3.12 (or 3.11)
python3.12 -m venv venv

# Activate (Mac/Linux)
source venv/bin/activate

# OR activate (Windows)
venv\Scripts\activate

# Verify Python version
python --version  # Should show Python 3.12.x
```

### Step 3: Install Dependencies

```bash
# Install Python dependencies
pip install -r requirements.txt

# Verify dbt installation
dbt --version  # Should show dbt-core 1.11.2 and snowflake 1.11.1
```

### Step 4: Configure Snowflake Connection

**Copy the profile template:**

```bash
cp profiles.yml.example profiles.yml
```

**Edit `profiles.yml` with your Snowflake credentials:**

```yaml
dbt_learning_sandbox:
  target: dev
  outputs:
    dev:
      type: snowflake
      
      # Slalom Snowflake account
      account: SLALOM-SNOWFLAKE_ILABS_QECATALYST
      
      # Your Slalom email address
      user: YOUR.EMAIL@SLALOM.COM
      
      # SSO authentication (browser-based)
      authenticator: externalbrowser
      
      # Your Snowflake role
      role: SNOWFLAKE-ILABS-QECATALYST
      
      # Your database (replace XX with your initials or use existing)
      database: CAPSTONE_FLOWERSHOP_DN_SKS
      
      # Warehouse for running queries
      warehouse: SALES_LOAD
      
      # Starting schema
      schema: RAW
      
      # Parallel threads
      threads: 4
      
      # Query tagging for audit trails
      query_tag: 'project:snowcommerce_analytics|env:dev'
```

### Step 5: Test Connection

```bash
# Debug connection
dbt debug

# Expected output:
# ✓ Connection used: dev
# ✓ All checks passed!
```

### Step 6: Install dbt Packages

```bash
# Install external dbt packages (dbt_expectations, etc.)
dbt deps

# Verify packages installed
ls -la dbt_packages/
```

### Step 7: Verify S3 Integration

Before running dbt, verify that Snowflake can access the S3 bucket with CSVs:

```sql
-- Test S3 access (run in Snowflake)
LIST @<your_stage>
-- Should show the CSV files uploaded to S3
```

Once verified, run the pipeline:

```bash
# This will:
# 1. dbt run     - Build all models (bronze tables created from S3 → silver views → gold tables)
# 2. dbt test    - Run 225+ tests
dbt build

# Expected: "Done. PASS=225 WARN=0 ERROR=0"
```

---

## Data Models & Schema

### Sample Data Overview

The project includes realistic e-commerce sample data with relationships:

```
Customers (10 rows)
└── Orders (20 rows)
    └── OrderItems (50 rows)
└── Products (15 rows)
    └── Marketing Campaigns (3 campaigns)
```

### Bronze Layer (`raw` schema)

**Sources defined in `models/bronze/sources.yml`**

**Data Location:** Snowflake external stage pointing to S3 bucket (CSVs uploaded)

**Creation:** Tables created from S3 external stage using Snowflake COPY or CREATE TABLE AS SELECT

#### 1. Customers Source

| Column | Type | Description | Tests |
|--------|------|-------------|-------|
| customer_id | INTEGER | Unique ID | unique, not_null |
| first_name | VARCHAR | First name | not_null |
| last_name | VARCHAR | Last name | not_null |
| email | VARCHAR | Email address | unique, not_null |
| phone | VARCHAR | Phone | |
| address | VARCHAR | Street address | |
| city | VARCHAR | City | |
| state | VARCHAR | State | |
| zip_code | VARCHAR | ZIP | |
| created_at | TIMESTAMP | Creation date | not_null |

#### 2. Orders Source

| Column | Type | Description | Tests |
|--------|------|-------------|-------|
| order_id | INTEGER | Unique order ID | unique, not_null |
| customer_id | INTEGER | FK to customers | not_null |
| order_date | DATE | Order date | not_null |
| order_status | VARCHAR | pending/processing/completed | accepted_values |
| total_amount | DECIMAL | Total order value | not_null |

#### 3. Products Source

| Column | Type | Description | Tests |
|--------|------|-------------|-------|
| product_id | INTEGER | Unique product ID | unique, not_null |
| product_name | VARCHAR | Product name | not_null |
| category | VARCHAR | Product category | not_null |
| price | DECIMAL | Product price | not_null |
| stock_quantity | INTEGER | Stock count | not_null |

---

### Silver Layer (`silver` schema)

**Materialization: VIEW** (lightweight, fast query time)

**Schema defined in `models/silver/_silver_schema.yml`**

#### stg_customers (10 columns)

**Transformations:**
- Removes null customer IDs
- Standardizes column names  
- Type validation

#### stg_orders (11 columns)

**Transformations:**
- Adds date dimensions (year, month, quarter)
- Creates reporting columns (day of week)
- Adds load timestamp

#### stg_products (6 columns)

**Transformations:**
- Removes null product IDs
- Standardizes names and categories
- Type validation

---

### Gold Layer (`gold` schema)

**Materialization: TABLE** (optimized for queries)

**Schema defined in `models/gold/_gold_schema.yml`**

#### dim_customers (13 columns)

**Dimension table:** One row per customer with calculated metrics

**Key columns:**
- Customer IDs, names, contact info
- Calculated: total orders, lifetime revenue, most recent order
- Segment: VIP/High Value/Standard based on lifetime value

#### fct_orders (20 columns)

**Fact table:** One row per order with denormalized customer and aggregated item data

**Key columns:**
- Order IDs, dates, status, totals
- Customer denormalization: names, email, segment
- Date dimensions: year, month, quarter
- Metrics: item count, order value category, days since order

#### rpt_customer_metrics (7 columns)

**Aggregated report:** Monthly customer activity metrics

**Key columns:**
- Customer ID and name
- Order year and month
- Calculated: order count, revenue, average order value

---

## Testing Framework

**225+ total tests** covering:
- Unique, not_null, accepted_values, relationships tests
- dbt_expectations column existence validation on all columns
- Custom SQL data quality tests

**Test types:**
- Generic tests (defined in YAML schema files)
- dbt_expectations (schema validation)
- Custom SQL tests (data quality)

**Running tests:**
```bash
dbt test              # Run all tests
dbt test --select dim_customers  # Specific model
dbt build             # Run + test
```

---

## Audit Trail & Query Tagging

**Query tags** capture metadata on every execution:
- Project name, model name, model type
- User who ran it, environment
- Used for cost tracking and compliance

**Execution hooks** log dbt run start/end with timestamps

**Audit table** (optional) stores execution history in Snowflake

See [AUDIT_TRAIL_SETUP.md](AUDIT_TRAIL_SETUP.md) for monitoring queries and cost analysis

---

## Running dbt

### Quick Commands

```bash
# Activate environment
source venv/bin/activate

# Install packages
dbt deps

# Build everything (seed + run + test)
dbt build

# Just run models
dbt run

# Just run tests
dbt test

# View documentation
dbt docs generate && dbt docs serve

# Debug connection
dbt debug
```

### Advanced Selectors

```bash
# Run specific model and its upstream dependencies
dbt run --select +dim_customers

# Run specific model and its downstream models
dbt run --select dim_customers+

# Run entire layer
dbt run --select silver

# Run all models except a tag
dbt run --exclude tag:excluded

# Run in fail-fast mode (stop on first error)
dbt run --fail-fast
```

### Build Strategy

```bash
# Option 1: Full build (recommended - requires data loaded in S3)
dbt build

# Option 2: Incremental build (faster for development)
dbt build --select +modified

# Option 3: Only rebuild gold layer
dbt run --select gold

# Option 4: Rebuild with fresh dependencies
dbt deps && dbt build --no-parse

# Option 5: Load S3 data then run (if macro exists)
dbt run-operation load_from_s3 && dbt build
```

---

## Sample Data

**Data in S3:** 10-20 rows per table (small, realistic e-commerce data)

| File | Rows | Purpose |
|------|------|---------|
| customers.csv | 10 | Customer profiles |
| orders.csv | 20 | Order transactions |
| order_items.csv | 50+ | Line items |
| products.csv | 15 | Product catalog |

**Data loaded via:**
- Snowflake storage integration (connected to S3)
- External stage configuration
- COPY INTO or CREATE TABLE AS command

---

## dbt Commands Reference

| Command | Purpose |
|---------|---------|
| `dbt run` | Build all models |
| `dbt test` | Run tests |
| `dbt build` | run + test |
| `dbt compile` | Generate SQL only |
| `dbt docs generate` | Build docs |
| `dbt debug` | Test connection |

**Building strategically:**
```bash
dbt build                    # Full build
dbt build --select gold      # Gold layer only
dbt build --select +modified # Changed models + dependencies
```

---

## Documentation & Observability

**Auto-generated docs:**
- Run `dbt docs generate` (creates catalog.json, manifest.json)
- View at http://127.0.0.1:8000 with `dbt docs serve`
- Shows: models, columns, tests, lineage, dependencies

**Output files:**
- `logs/dbt.log` - Execution log
- `target/manifest.json` - Project dependency graph
- `target/run_results.json` - Test results
- `target/catalog.json` - Data dictionary

---

## Best Practices

**Naming:**
- `stg_` = staging models (silver)
- `dim_` = dimension tables (gold)
- `fct_` = fact tables (gold)
- `rpt_` = reports (gold)

**Structure:**
- Uppercase SQL keywords, lowercase identifiers
- One clause per line, use aliases
- Document all models and key columns

**Testing:**
- Bronze: Unique/not_null on keys
- Silver: Add relationships, casting validation
- Gold: Add business rules, referential integrity

**Materialization:**
- Views (silver) = fast iteration, no storage
- Tables (gold) = optimized for queries

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| **Connection failed** | Run `dbt debug` to test Snowflake connection; verify profiles.yml and credentials |
| **Permission denied** | Check role has database/schema grants in Snowflake; verify role in profiles.yml |
| **Source not found** | Verify source defined in `models/bronze/sources.yml`; check YAML syntax |
| **dbt_expectations not found** | Run `dbt deps` to install packages; verify packages.yml includes dbt_expectations |
| **Test failures** | View SQL in `target/compiled/`; manually run test query in Snowflake; fix data or logic |
| **Circular dependencies** | Review model references; break cycles with ephemeral models or refactor |
| **Warehouse timeout** | Check warehouse is running; upgrade size if needed; optimize model queries |

**General tips:** Run `dbt debug` for connection issues, `dbt parse` for YAML syntax errors, `dbt run --full-refresh` to rebuild all models, `dbt test --debug` for test details.


## Next Steps

1. Run `dbt build` to validate setup
2. View docs at http://127.0.0.1:8000
3. Study existing models in `models/gold/`
4. Create a new model and add tests
5. Re-run `dbt build` and validate

**Resources:** [dbt Docs](https://docs.getdbt.com/) | [Snowflake Docs](https://docs.snowflake.com/) | [AUDIT_TRAIL_SETUP.md](AUDIT_TRAIL_SETUP.md)

---

## License

MIT License - Use for learning and teaching

---

**Last Updated:** March 16, 2026 | **dbt Version:** 1.11.2 | **Snowflake Adapter:** 1.11.1

For detailed audit trail setup: see [AUDIT_TRAIL_SETUP.md](AUDIT_TRAIL_SETUP.md)

For quick start: see [QUICKSTART.md](QUICKSTART.md)
