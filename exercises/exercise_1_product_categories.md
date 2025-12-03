# Exercise 1: Product Categories Analytics

## 🎯 Learning Objectives

By completing this exercise, you will learn how to:

- Create seed files (Bronze layer)
- Define sources in YAML configuration
- Build staging models using `{{ source() }}` (Silver layer)
- Write schema tests for data quality
- Create dimension tables and reports using `{{ ref() }}` (Gold layer)
- Run the complete dbt workflow

---

## 📋 Scenario

The e-commerce team wants to analyze sales by product category. Currently, products exist in the database but there's no category information. Your task is to:

1. Add category data to the Bronze layer
2. Clean and stage it in the Silver layer
3. Create a dimension table and sales report in the Gold layer

---

## Part 1: Bronze Layer (Raw Data)

### Step 1.1: Create the Seed File

Create a new file called `categories.csv` in the `seeds/` folder.

**Requirements:**

- Include these columns: `category_id`, `category_name`, `department`, `created_at`
- Create at least 5 categories that make sense for an e-commerce store
- Use realistic data (e.g., Electronics, Clothing, Home & Garden, etc.)

**Hints:**

- Look at existing seed files like `products.csv` for format reference
- Use consistent date formatting (YYYY-MM-DD)
- `category_id` should be unique integers

<details>
<summary>💡 Example Data Structure (click to expand if stuck)</summary>

```
category_id,category_name,department,created_at
1,Laptops,Electronics,2023-01-15
2,Smartphones,Electronics,2023-01-15
...
```

</details>

### Step 1.2: Load the Seed Data

Run the following command to load your CSV into Snowflake:

```bash
dbt seed
```

**Verify it worked:**

```bash
dbt seed --select categories
```

You should see output indicating the `categories` table was created in the `raw` schema.

### Step 1.3: Define the Source

Open `models/bronze/sources.yml` and add your new `categories` table to the sources configuration.

**Requirements:**

- Add the table under the existing `raw` source
- Include a description for the table
- Add descriptions for each column

**Hints:**

- Look at how other tables (like `customers` or `products`) are defined
- Follow the same YAML structure and indentation

<details>
<summary>💡 YAML Structure Hint (click to expand if stuck)</summary>

```yaml
tables:
  - name: your_table_name
    description: 'Your description here'
    columns:
      - name: column_name
        description: 'Column description'
```

</details>

---

## Part 2: Silver Layer (Staging)

### Step 2.1: Create the Staging Model

Create a new file called `stg_categories.sql` in `models/silver/`.

**Requirements:**

- Select from the source using `{{ source('raw', 'categories') }}`
- Rename columns to follow naming conventions if needed
- Cast data types appropriately (especially dates)
- Add any data cleaning (trim whitespace, standardize case, etc.)
- Include a model configuration for materialization as a `view`

**Hints:**

- Look at existing staging models like `stg_products.sql` for patterns
- Use `TRIM()` to clean string fields
- Use `CAST()` or `::` for type conversions
- Use `UPPER()` or `LOWER()` if you want consistent casing

<details>
<summary>💡 Basic Structure Hint (click to expand if stuck)</summary>

```sql
{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'table_name') }}
),

cleaned AS (
    SELECT
        -- your columns here with transformations
    FROM source
)

SELECT * FROM cleaned
```

</details>

### Step 2.2: Add Schema Tests

Open `models/silver/_silver_schema.yml` and add tests for your staging model.

**Requirements:**

- Add the model to the YAML file
- Add `unique` and `not_null` tests for `category_id`
- Add `not_null` test for `category_name`
- Add any other tests you think are appropriate

**Hints:**

- Look at how other staging models are tested in the same file
- Think about what data quality rules make sense for categories

### Step 2.3: Run and Test

```bash
# Build the staging model
dbt run --select stg_categories

# Run tests on your model
dbt test --select stg_categories
```

Fix any errors before proceeding!

---

## Part 3: Gold Layer (Business Logic)

### Step 3.1: Create the Dimension Table

Create a new file called `dim_categories.sql` in `models/gold/`.

**Requirements:**

- Select from the staging model using `{{ ref('stg_categories') }}`
- Configure as a `table` materialization
- Add a surrogate key or use the existing category_id as primary key
- Include all relevant business columns
- Add any calculated fields that might be useful

**Hints:**

- Dimension tables are typically materialized as `table` for performance
- Consider what attributes analysts would want to filter/group by

<details>
<summary>💡 Basic Structure Hint (click to expand if stuck)</summary>

```sql
{{ config(materialized='table') }}

SELECT
    -- your dimension columns
FROM {{ ref('stg_categories') }}
```

</details>

### Step 3.2: Create a Sales by Category Report

Create a new file called `rpt_sales_by_category.sql` in `models/gold/`.

**Requirements:**

- Join categories with products and order_items to calculate sales
- Aggregate total revenue and order count per category
- Include category name and department
- Configure as a `table` materialization

**You'll need to join these models:**

- `{{ ref('dim_categories') }}` - your new dimension
- `{{ ref('stg_products') }}` or `{{ ref('dim_products') }}` - to link products to categories
- `{{ ref('fct_order_items') }}` or `{{ ref('stg_order_items') }}` - for sales data

**Challenge:** You'll need to figure out how to link categories to products. Look at the existing `products.csv` - does it have a category field? If not, you may need to:

- Option A: Add a `category_id` column to `products.csv` and rebuild
- Option B: Create a mapping based on product names (more advanced)

**Hints:**

- Start simple - get the joins working first, then add aggregations
- Use `SUM()` for revenue, `COUNT()` for order counts
- Group by category dimensions

<details>
<summary>💡 Join Pattern Hint (click to expand if stuck)</summary>

```sql
SELECT
    c.category_name,
    c.department,
    COUNT(DISTINCT oi.order_id) AS total_orders,
    SUM(oi.quantity) AS total_units_sold,
    SUM(oi.line_total) AS total_revenue
FROM {{ ref('dim_categories') }} c
LEFT JOIN {{ ref('...') }} p ON ...
LEFT JOIN {{ ref('...') }} oi ON ...
GROUP BY 1, 2
```

</details>

### Step 3.3: Add Gold Layer Schema

Open `models/gold/_gold_schema.yml` and add your new models.

**Requirements:**

- Add `dim_categories` with appropriate tests
- Add `rpt_sales_by_category` with descriptions
- Include column descriptions

---

## Part 4: Validation

### Step 4.1: Build Everything

Run the complete build to ensure everything works together:

```bash
# Build just your new models
dbt build --select +rpt_sales_by_category

# Or build everything
dbt build
```

The `+` prefix means "build this model and all its upstream dependencies."

### Step 4.2: Generate Documentation

```bash
dbt docs generate
dbt docs serve
```

Navigate to your models in the documentation site. Can you see:

- The lineage graph showing Bronze → Silver → Gold?
- Your model and column descriptions?
- Test results?

### Step 4.3: Query Your Results

Connect to Snowflake and run:

```sql
-- Check your dimension table
SELECT * FROM gold.dim_categories;

-- Check your report
SELECT * FROM gold.rpt_sales_by_category ORDER BY total_revenue DESC;
```

---

## ✅ Completion Checklist

Before considering this exercise complete, verify:

- [ ] `seeds/categories.csv` exists with valid data
- [ ] `dbt seed` runs successfully
- [ ] Source is defined in `models/bronze/sources.yml`
- [ ] `models/silver/stg_categories.sql` exists and builds
- [ ] Staging tests pass (`dbt test --select stg_categories`)
- [ ] `models/gold/dim_categories.sql` exists and builds
- [ ] `models/gold/rpt_sales_by_category.sql` exists and builds
- [ ] All tests pass (`dbt test`)
- [ ] Documentation is generated and viewable
- [ ] You can query the final report in Snowflake

---

## 🚀 Bonus Challenges

If you finish early, try these extensions:

1. **Add more tests:** Use `dbt_utils.accepted_values` to validate department names
2. **Add category hierarchy:** Create parent/child category relationships
3. **Time-based analysis:** Create `rpt_category_sales_by_month.sql`
4. **Rank categories:** Add a `category_rank` based on total revenue

---

## 🆘 Troubleshooting

### "Source not found" error

- Make sure you ran `dbt seed` first
- Check that the source name in `sources.yml` matches exactly

### "Model not found" error when using `{{ ref() }}`

- Check the model name matches the filename (without .sql)
- Make sure the upstream model builds successfully first

### Tests failing

- Run `dbt test --select model_name` to see detailed error messages
- Check for NULL values or duplicates in your data

### Joins returning no results

- Build and query each model individually first
- Check that join keys match (same data type, same values)
