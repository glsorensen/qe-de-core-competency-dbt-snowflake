# 🏔️ dbt + Snowflake Tutorial: Medallion Architecture

A beginner-friendly tutorial project demonstrating **dbt** (data build tool) with **Snowflake** using the **Medallion Architecture** pattern (Bronze → Silver → Gold).

## 📚 What You'll Learn

- **dbt Fundamentals**: Sources, models, refs, tests, and documentation
- **Medallion Architecture**: Industry-standard data lakehouse pattern
- **Data Transformations**: Clean, transform, and aggregate data
- **Data Quality**: Testing and validation at each layer
- **Snowflake**: Cloud data warehouse basics

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                     MEDALLION ARCHITECTURE                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐             │
│  │   BRONZE    │───▶│   SILVER    │───▶│    GOLD     │             │
│  │   (Raw)     │    │  (Cleaned)  │    │ (Business)  │             │
│  └─────────────┘    └─────────────┘    └─────────────┘             │
│                                                                     │
│  • Raw source data   • Standardized     • Dimensions (dim_*)       │
│  • No transforms     • Type casting     • Facts (fct_*)            │
│  • sources.yml       • Deduplication    • Reports (rpt_*)          │
│                      • stg_* models     • Business logic           │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## 📁 Project Structure

```
dbt-project/
├── models/
│   ├── bronze/           # Raw data source definitions
│   │   └── sources.yml   # Source table configurations
│   ├── silver/           # Cleaned staging models
│   │   ├── stg_customers.sql
│   │   ├── stg_orders.sql
│   │   ├── stg_order_items.sql
│   │   ├── stg_products.sql
│   │   └── stg_marketing_campaigns.sql
│   └── gold/             # Business-ready models
│       ├── dim_customers.sql        # Customer dimension
│       ├── fct_orders.sql           # Orders fact table
│       ├── fct_order_items.sql      # Order items fact table
│       ├── rpt_customer_metrics.sql # Customer analytics report
│       └── rpt_product_performance.sql
├── seeds/                # Sample CSV data (included)
├── macros/               # Reusable SQL functions
├── dbt_project.yml       # Project configuration
├── packages.yml          # dbt package dependencies
└── profiles.yml.example  # Connection template
```

## 🚀 Quick Start

### Prerequisites

1. **Python 3.11** installed (dbt doesn't support 3.13+ yet)
2. **Slalom Snowflake access** (SSO via your @slalom.com email)

### Step 1: Clone the Repository

```bash
git clone https://github.com/glsorensen/qe-de-core-competency-dbt-snowflake.git
cd qe-de-core-competency-dbt-snowflake
```

### Step 2: Install dbt

```bash
# Create virtual environment with Python 3.11
python3.11 -m venv venv

# Activate it
source venv/bin/activate  # Mac/Linux
# venv\Scripts\activate   # Windows

# Install dependencies
pip install -r requirements.txt
```

### Step 3: Configure Snowflake Connection

1. Copy the example profile:

```bash
mkdir -p ~/.dbt
cp profiles.yml.example ~/.dbt/profiles.yml
```

2. Edit `~/.dbt/profiles.yml` with your Snowflake credentials:

```yaml
dbt_learning_sandbox:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: SLALOM-SNOWFLAKE_ILABS_QECATALYST
      user: YOUR_EMAIL@SLALOM.COM # Your Slalom email
      authenticator: externalbrowser # Enables SSO login
      role: PUBLIC
      database: DBT_LEARNING
      warehouse: SALES_LOAD
      schema: RAW
      threads: 4
```

### Step 4: Verify Snowflake Access

The database `DBT_LEARNING` and warehouse `SALES_LOAD` are already configured for Slalom users. You can verify access by running:

```bash
dbt debug
```

You should see "All checks passed!" ✅

> **Note:** If you're setting up your own Snowflake account, you'll need to create the database and schemas:
>
> ```sql
> CREATE DATABASE IF NOT EXISTS DBT_LEARNING;
> CREATE SCHEMA IF NOT EXISTS DBT_LEARNING.RAW;
> CREATE SCHEMA IF NOT EXISTS DBT_LEARNING.SILVER;
> CREATE SCHEMA IF NOT EXISTS DBT_LEARNING.GOLD;
> ```

### Step 5: Run the Tutorial!

```bash
# Install dbt packages
dbt deps

# Load sample data, build models, and run tests
dbt build
```

You should see: `Done. PASS=71 WARN=0 ERROR=0` ✅

## 📖 Understanding the Layers

### 🥉 Bronze Layer (Raw)

**Location**: `models/bronze/sources.yml`

The bronze layer defines references to raw source data. No transformations happen here - it's just pointing to where your data lives.

```yaml
sources:
  - name: raw
    tables:
      - name: customers
      - name: orders
      - name: products
```

**Key Concept**: Use `{{ source('raw', 'customers') }}` to reference raw tables.

---

### 🥈 Silver Layer (Cleaned/Staging)

**Location**: `models/silver/stg_*.sql`

The silver layer cleans and standardizes raw data:

- Rename columns for consistency
- Cast data types
- Handle nulls
- Apply basic business rules
- Filter invalid records

**Example** (`stg_customers.sql`):

```sql
SELECT
    customer_id,
    TRIM(INITCAP(first_name)) AS first_name,  -- Standardize names
    TRIM(LOWER(email)) AS email,               -- Lowercase emails
    DATEDIFF('year', date_of_birth, CURRENT_DATE()) AS customer_age
FROM {{ source('raw', 'customers') }}
WHERE customer_id IS NOT NULL  -- Filter bad data
```

**Key Concept**: Use `{{ source() }}` to read from bronze, output to silver schema.

---

### 🥇 Gold Layer (Business-Ready)

**Location**: `models/gold/dim_*.sql`, `fct_*.sql`, `rpt_*.sql`

The gold layer contains business-ready, analytics-focused models:

| Prefix | Purpose          | Example                                     |
| ------ | ---------------- | ------------------------------------------- |
| `dim_` | Dimension tables | `dim_customers` - Customer attributes       |
| `fct_` | Fact tables      | `fct_orders` - Order transactions           |
| `rpt_` | Report tables    | `rpt_customer_metrics` - Aggregated metrics |

**Example** (`dim_customers.sql`):

```sql
SELECT
    c.customer_id,
    c.full_name,
    COUNT(o.order_id) AS total_orders,
    SUM(o.total_amount) AS lifetime_revenue,
    CASE
        WHEN SUM(o.total_amount) > 1000 THEN 'vip'
        WHEN SUM(o.total_amount) > 500 THEN 'high_value'
        ELSE 'standard'
    END AS customer_tier
FROM {{ ref('stg_customers') }} c
LEFT JOIN {{ ref('stg_orders') }} o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.full_name
```

**Key Concept**: Use `{{ ref() }}` to reference other dbt models.

## 🧪 Testing

dbt includes powerful testing capabilities:

```bash
# Run all tests
dbt test

# Run tests for specific model
dbt test --select dim_customers

# Run tests for a layer
dbt test --select silver
```

Tests are defined in schema files (`_silver_schema.yml`, `_gold_schema.yml`):

```yaml
models:
  - name: stg_customers
    columns:
      - name: customer_id
        tests:
          - unique
          - not_null
      - name: email
        tests:
          - unique
```

## 📊 Common dbt Commands

| Command                           | Description                                     |
| --------------------------------- | ----------------------------------------------- |
| `dbt run`                         | Execute all models                              |
| `dbt run --select silver`         | Run only silver layer                           |
| `dbt run --select +dim_customers` | Run dim_customers and all upstream dependencies |
| `dbt test`                        | Run all tests                                   |
| `dbt build`                       | Run + test in dependency order                  |
| `dbt docs generate`               | Generate documentation                          |
| `dbt docs serve`                  | View documentation in browser                   |
| `dbt seed`                        | Load CSV seed files                             |
| `dbt debug`                       | Test connection and configuration               |

## 🎯 Learning Exercises

### Exercise 1: Add a New Source Column

1. Add a `loyalty_points` column to `sources.yml`
2. Include it in `stg_customers.sql`
3. Run `dbt run --select stg_customers`

### Exercise 2: Create a New Dimension

1. Create `dim_products.sql` in the gold layer
2. Add product metrics (total sales, avg discount)
3. Add tests in `_gold_schema.yml`

### Exercise 3: Build a Report

1. Create `rpt_monthly_sales.sql`
2. Aggregate orders by month
3. Include revenue, order count, avg order value

## 🔧 Troubleshooting

### "Profile not found"

Make sure `~/.dbt/profiles.yml` exists and the profile name matches `dbt_project.yml`.

### "Database does not exist"

Run the Snowflake setup SQL commands from Step 4.

### "Permission denied"

Ensure your Snowflake role has proper grants on all schemas.

### "Source not found"

Run `dbt seed` first to load sample data into the RAW schema.

## 📚 Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [Snowflake Documentation](https://docs.snowflake.com/)
- [Medallion Architecture Guide](https://www.databricks.com/glossary/medallion-architecture)
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)

## 🤝 Contributing

Feel free to submit issues and pull requests to improve this tutorial!

## 📝 License

MIT License - feel free to use this for learning and teaching!
