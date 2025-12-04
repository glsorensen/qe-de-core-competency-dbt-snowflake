# Exercise 0: Getting Started with dbt

## 🎯 Learning Objectives

By completing this exercise, you will learn how to:

- Run basic dbt commands
- Understand the Medallion Architecture (Bronze → Silver → Gold)
- Explore existing models and their relationships
- View dbt documentation and lineage graphs

**No coding required!** This exercise is all about exploring and understanding.

---

## 📋 Prerequisites

Before starting, make sure you have:

- [ ] Completed the setup in [QUICKSTART.md](../QUICKSTART.md)
- [ ] A working connection to Snowflake
- [ ] dbt installed and configured

---

## Part 1: Explore the Project Structure

### Step 1.1: Look Around

Take a few minutes to explore the folder structure:

```
dbt-project/
├── seeds/           ← Bronze: Raw CSV data files
├── models/
│   ├── bronze/      ← Source definitions (where raw data lives)
│   ├── silver/      ← Staging models (cleaned data)
│   └── gold/        ← Business-ready models (reports & dimensions)
└── dbt_project.yml  ← Project configuration
```

**🤔 Questions to answer:**

1. How many CSV files are in the `seeds/` folder?
2. How many staging models (files starting with `stg_`) are in `models/silver/`?
3. What types of models are in `models/gold/`? (hint: look at prefixes like `dim_`, `fct_`, `rpt_`)

<details>
<summary>💡 Answers (click to expand)</summary>

1. There are 5 seed files: `customers.csv`, `orders.csv`, `order_items.csv`, `products.csv`, `marketing_campaigns.csv`
2. There are 5 staging models: `stg_customers.sql`, `stg_orders.sql`, `stg_order_items.sql`, `stg_products.sql`, `stg_marketing_campaigns.sql`
3. Gold models include:
   - `dim_` (dimension): `dim_customers.sql`
   - `fct_` (fact): `fct_orders.sql`, `fct_order_items.sql`
   - `rpt_` (report): `rpt_customer_metrics.sql`, `rpt_product_performance.sql`

</details>

---

## Part 2: Run Your First dbt Commands

### Step 2.1: Check Your Connection

First, let's make sure dbt can connect to Snowflake:

```bash
dbt debug
```

✅ **Success looks like:** All checks should show "OK" in green.

❌ **If it fails:** Check your `profiles.yml` configuration.

### Step 2.2: Load the Seed Data

Load the CSV files into Snowflake (Bronze layer):

```bash
dbt seed
```

✅ **Success looks like:** You should see each seed file being loaded with row counts.

**🤔 Question:** How many rows were loaded into the `customers` table?

### Step 2.3: Build the Models

Now build all the Silver and Gold models:

```bash
dbt run
```

✅ **Success looks like:** Each model should show "SUCCESS" with timing information.

**🤔 Questions:**

1. How many models were built?
2. Which models are built as `view` vs `table`? (hint: look at the output)

### Step 2.4: Run the Tests

Check that the data passes quality tests:

```bash
dbt test
```

✅ **Success looks like:** All tests should pass.

**🤔 Question:** How many tests were run?

---

## Part 3: View the Documentation

### Step 3.1: Generate and Serve Docs

dbt can generate a beautiful documentation website:

```bash
dbt docs generate
dbt docs serve
```

This will open a browser window with your project documentation.

### Step 3.2: Explore the Lineage Graph

In the documentation site:

1. Click on any model (try `fct_orders`)
2. Look at the bottom-right corner for the **Lineage Graph** button (it looks like a branching icon)
3. Click it to see how models connect

**🤔 Questions:**

1. What does `fct_orders` depend on? (What are its "parents"?)
2. What depends on `stg_customers`? (What are its "children"?)

<details>
<summary>💡 Answers (click to expand)</summary>

1. `fct_orders` depends on `stg_orders` and `stg_customers`
2. `stg_customers` is used by `dim_customers`, `fct_orders`, and `rpt_customer_metrics`

</details>

---

## Part 4: Query the Results in Snowflake

### Step 4.1: Connect to Snowflake

Open your Snowflake console and run these queries to see the Medallion layers:

```sql
-- Bronze Layer (raw data)
SELECT * FROM raw.customers LIMIT 5;

-- Silver Layer (cleaned/staged data)
SELECT * FROM silver.stg_customers LIMIT 5;

-- Gold Layer (business-ready data)
SELECT * FROM gold.dim_customers LIMIT 5;
```

**🤔 Questions:**

1. What differences do you notice between `raw.customers` and `silver.stg_customers`?
2. What extra columns does `gold.dim_customers` have that aren't in the staging model?

<details>
<summary>💡 Things to Look For (click to expand)</summary>

**Silver layer transformations:**

- Names are standardized (INITCAP)
- Email is lowercase
- Phone numbers are cleaned (non-numeric removed)
- Calculated fields like `customer_age` and `days_since_signup`

**Gold layer additions:**

- Business logic applied
- Additional aggregations or categorizations
- Ready-to-use for analytics

</details>

---

## Part 5: Understand the Key Concepts

### The Medallion Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    MEDALLION ARCHITECTURE                    │
├─────────────┬─────────────────┬─────────────────────────────┤
│   BRONZE    │     SILVER      │            GOLD             │
│   (Raw)     │   (Staging)     │    (Business-Ready)         │
├─────────────┼─────────────────┼─────────────────────────────┤
│ CSV files   │ Clean & filter  │ Join & aggregate            │
│ "As-is"     │ Standardize     │ Business logic              │
│ No changes  │ Type casting    │ Ready for dashboards        │
└─────────────┴─────────────────┴─────────────────────────────┘
```

### The Two Magic Functions

**`{{ source() }}`** - Used in Silver layer to reference Bronze (raw) data:

```sql
SELECT * FROM {{ source('raw', 'customers') }}
```

**`{{ ref() }}`** - Used in Silver/Gold to reference other dbt models:

```sql
SELECT * FROM {{ ref('stg_customers') }}
```

---

## ✅ Completion Checklist

Before moving on to Exercise 1, verify:

- [ ] `dbt debug` passes all checks
- [ ] `dbt seed` loads all CSV files successfully
- [ ] `dbt run` builds all models successfully
- [ ] `dbt test` passes all tests
- [ ] You can view the documentation site
- [ ] You understand the lineage graph
- [ ] You can query Bronze, Silver, and Gold tables in Snowflake
- [ ] You understand when to use `{{ source() }}` vs `{{ ref() }}`

---

## 🎉 Congratulations!

You've completed Exercise 0! You now understand:

- ✅ How to run dbt commands
- ✅ The Medallion Architecture (Bronze → Silver → Gold)
- ✅ How to explore model dependencies
- ✅ How data transforms through each layer

**Next step:** Move on to [Exercise 1: Product Categories](./exercise_1_product_categories.md) where you'll create your first models!

---

## 📚 Quick Reference

| Command             | What it does                   |
| ------------------- | ------------------------------ |
| `dbt debug`         | Test your connection           |
| `dbt seed`          | Load CSV files into Snowflake  |
| `dbt run`           | Build all models               |
| `dbt test`          | Run data quality tests         |
| `dbt build`         | seed + run + test (all in one) |
| `dbt docs generate` | Create documentation           |
| `dbt docs serve`    | View documentation in browser  |
