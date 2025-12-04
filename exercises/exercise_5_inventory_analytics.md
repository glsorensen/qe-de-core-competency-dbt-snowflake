# Exercise 5: Inventory & Supply Chain Analytics (Advanced)

## 🎯 Learning Objectives

By completing this exercise, you will learn how to:

- Design complex multi-table seed data with temporal relationships
- Implement **Slowly Changing Dimensions (SCD Type 2)** for historical tracking
- Use **window functions** for running totals, rankings, and time-series analysis
- Create **incremental models** for efficient processing
- Build **custom macros** for reusable business logic
- Implement **advanced dbt tests** including custom generic tests
- Create a **multi-metric dashboard report** with complex aggregations
- Handle **data anomaly detection** patterns

---

## 📋 Scenario

The operations team needs a comprehensive inventory and supply chain analytics solution. They need to:

1. Track inventory levels over time with historical snapshots
2. Identify stockout risks and reorder points
3. Analyze supplier performance and lead times
4. Calculate inventory turnover and days of supply
5. Detect anomalies in inventory movements

**Business Context:**

- Products have multiple suppliers with different lead times and costs
- Inventory levels change daily through sales, restocks, and adjustments
- Historical inventory data is needed for trend analysis and forecasting
- Supplier performance impacts purchasing decisions

---

## Part 1: Bronze Layer (Complex Seed Data)

### Step 1.1: Create Suppliers Seed File

Create `seeds/suppliers.csv` with supplier information.

**Required Columns:**

- `supplier_id` - Unique identifier
- `supplier_name` - Company name
- `contact_email` - Primary contact
- `country` - Supplier location
- `lead_time_days` - Average delivery lead time
- `reliability_score` - Historical reliability (0.0-1.0)
- `payment_terms_days` - Net payment terms
- `is_preferred` - Preferred supplier flag (true/false)
- `contract_start_date` - When relationship began
- `contract_end_date` - Contract expiration (NULL if ongoing)

**Requirements:**

- Create at least 6 suppliers
- Mix of domestic and international suppliers
- Varying lead times (3-30 days)
- Some preferred, some not
- At least one with expired contract

<details>
<summary>💡 Sample Data Structure (click to expand if stuck)</summary>

```csv
supplier_id,supplier_name,contact_email,country,lead_time_days,reliability_score,payment_terms_days,is_preferred,contract_start_date,contract_end_date
1,TechSource Global,orders@techsource.com,USA,5,0.95,30,true,2022-01-01,
2,Pacific Components,sales@pacificcomp.cn,China,21,0.82,45,false,2022-06-15,
...
```

</details>

### Step 1.2: Create Product-Supplier Mapping

Create `seeds/product_suppliers.csv` to link products with their suppliers.

**Required Columns:**

- `product_supplier_id` - Unique identifier
- `product_id` - Reference to products table
- `supplier_id` - Reference to suppliers table
- `unit_cost` - Cost from this supplier
- `minimum_order_quantity` - MOQ requirement
- `is_primary_supplier` - Primary supplier for this product (only one per product)
- `effective_date` - When this relationship became effective

**Requirements:**

- Each product should have 1-3 suppliers
- Only ONE primary supplier per product
- Different suppliers may have different costs for the same product
- Use valid `product_id` values from `products.csv`

<details>
<summary>💡 Sample Data Structure (click to expand if stuck)</summary>

```csv
product_supplier_id,product_id,supplier_id,unit_cost,minimum_order_quantity,is_primary_supplier,effective_date
1,1,1,32.00,50,true,2023-01-01
2,1,2,28.50,200,false,2023-03-15
3,2,3,10.00,100,true,2023-01-01
...
```

</details>

### Step 1.3: Create Inventory Transactions

Create `seeds/inventory_transactions.csv` for inventory movements.

**Required Columns:**

- `transaction_id` - Unique identifier
- `product_id` - Reference to products
- `transaction_type` - 'restock', 'sale', 'adjustment', 'return', 'damaged'
- `quantity` - Positive for additions, negative for reductions
- `unit_cost` - Cost at time of transaction
- `transaction_date` - When the movement occurred
- `reference_id` - Order ID for sales, PO number for restocks, etc.
- `notes` - Optional notes

**Requirements:**

- Create at least 50 transactions spanning 6+ months
- Include all transaction types
- Mix of products
- Create realistic patterns (restocks followed by sales depleting inventory)
- Include some anomalies (unusual large adjustments, damaged goods)

**Think about:**

- Sales should reference order_ids from `orders.csv`
- Restocks should bring inventory up
- Include some negative adjustments (damaged, lost items)

<details>
<summary>💡 Transaction Pattern Hints</summary>

A realistic pattern might look like:

1. Initial restock of 100 units
2. Several sales reducing inventory
3. Restock when inventory gets low
4. Occasional adjustment for damaged goods
5. Some returns adding inventory back

```csv
transaction_id,product_id,transaction_type,quantity,unit_cost,transaction_date,reference_id,notes
1,1,restock,100,35.00,2023-06-01,PO-001,Initial stock
2,1,sale,-2,35.00,2023-06-15,ORD-001,
3,1,sale,-1,35.00,2023-06-20,ORD-002,
4,1,damaged,-3,35.00,2023-07-01,,Found defective units
...
```

</details>

### Step 1.4: Create Inventory Snapshots (Historical)

Create `seeds/inventory_snapshots.csv` for daily inventory levels.

**Required Columns:**

- `snapshot_id` - Unique identifier
- `product_id` - Reference to products
- `snapshot_date` - Date of the snapshot
- `quantity_on_hand` - Units available
- `quantity_reserved` - Units allocated to pending orders
- `quantity_available` - On hand minus reserved
- `reorder_point` - Threshold for reordering
- `reorder_quantity` - How much to order when below threshold

**Requirements:**

- Create weekly snapshots for at least 3 months
- Multiple products per snapshot date
- Show realistic inventory fluctuations
- Some products should hit zero (stockout)
- Include reorder points based on product velocity

<details>
<summary>💡 Snapshot Generation Pattern</summary>

For each week, calculate the inventory level based on transactions:

- Starting inventory + restocks - sales - adjustments = ending inventory

```csv
snapshot_id,product_id,snapshot_date,quantity_on_hand,quantity_reserved,quantity_available,reorder_point,reorder_quantity
1,1,2023-06-01,100,5,95,20,50
2,1,2023-06-08,87,3,84,20,50
3,1,2023-06-15,72,8,64,20,50
...
```

</details>

### Step 1.5: Load Seeds and Define Sources

```bash
dbt seed
```

Update `models/bronze/sources.yml` to add all four new tables:

- `suppliers`
- `product_suppliers`
- `inventory_transactions`
- `inventory_snapshots`

Include comprehensive column descriptions and consider adding `freshness` checks.

---

## Part 2: Silver Layer (Complex Transformations)

### Step 2.1: Create Staging Models

Create the following staging models in `models/silver/`:

#### `stg_suppliers.sql`

**Requirements:**

- Standard data cleaning (trim, case normalization)
- Calculate `is_active_contract` based on contract dates
- Calculate `contract_days_remaining` (NULL if no end date)
- Validate `reliability_score` is between 0 and 1
- Categorize lead time: 'fast' (≤7), 'medium' (8-14), 'slow' (>14)

<details>
<summary>💡 Lead Time Category Logic</summary>

```sql
CASE
    WHEN lead_time_days <= 7 THEN 'fast'
    WHEN lead_time_days <= 14 THEN 'medium'
    ELSE 'slow'
END AS lead_time_category
```

</details>

#### `stg_product_suppliers.sql`

**Requirements:**

- Join with products and suppliers to validate foreign keys
- Calculate `cost_variance_from_retail` (product price - unit cost)
- Calculate `margin_percentage`
- Flag if unit cost exceeds product retail price (data quality issue)

<details>
<summary>💡 Margin Calculation</summary>

```sql
-- Get product price from stg_products
ROUND(((p.price - ps.unit_cost) / NULLIF(p.price, 0)) * 100, 2) AS margin_percentage
```

</details>

#### `stg_inventory_transactions.sql`

**Requirements:**

- Standardize transaction types to lowercase
- Ensure quantity sign matches transaction type (sales should be negative)
- Calculate `transaction_value` (quantity × unit_cost)
- Add `is_inbound` flag (restock, return = true)
- Parse reference_id to extract order numbers
- Add running total using window function (per product)

**Advanced - Running Total:**
Use a window function to calculate running inventory balance per product:

<details>
<summary>💡 Running Total Window Function</summary>

```sql
SUM(quantity) OVER (
    PARTITION BY product_id
    ORDER BY transaction_date, transaction_id
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS running_inventory_balance
```

</details>

#### `stg_inventory_snapshots.sql`

**Requirements:**

- Calculate `days_of_supply` based on average daily sales velocity
- Add `is_below_reorder_point` flag
- Add `is_stockout` flag (quantity_available = 0)
- Calculate week-over-week change in inventory
- Add `inventory_health` status: 'critical' (stockout), 'low' (below reorder), 'healthy' (above reorder), 'overstocked' (>3x reorder)

**Advanced - Week-over-Week Change:**

<details>
<summary>💡 WoW Change with LAG()</summary>

```sql
quantity_on_hand - LAG(quantity_on_hand, 1) OVER (
    PARTITION BY product_id
    ORDER BY snapshot_date
) AS wow_quantity_change,

ROUND(
    (quantity_on_hand - LAG(quantity_on_hand, 1) OVER (
        PARTITION BY product_id
        ORDER BY snapshot_date
    )) * 100.0 / NULLIF(LAG(quantity_on_hand, 1) OVER (
        PARTITION BY product_id
        ORDER BY snapshot_date
    ), 0),
    2
) AS wow_change_percentage
```

</details>

### Step 2.2: Add Comprehensive Tests

Update `models/silver/_silver_schema.yml` with advanced tests:

**Required Tests:**

- Standard uniqueness and not_null tests
- `relationships` tests for all foreign keys
- `accepted_values` for transaction types and categories
- `dbt_utils.accepted_range` for scores and percentages

**Advanced Tests to Implement:**

1. **Custom test: Only one primary supplier per product**

Create `tests/generic/test_single_primary_supplier.sql`:

```sql
{% test single_primary_supplier(model) %}

SELECT
    product_id,
    COUNT(*) AS primary_count
FROM {{ model }}
WHERE is_primary_supplier = TRUE
GROUP BY product_id
HAVING COUNT(*) > 1

{% endtest %}
```

2. **Expression test: Inventory transactions should balance**

```yaml
tests:
  - dbt_utils.expression_is_true:
      expression: 'quantity_available = quantity_on_hand - quantity_reserved'
```

3. **Recency test: Snapshots should be recent**

```yaml
tests:
  - dbt_utils.recency:
      datepart: day
      field: snapshot_date
      interval: 14
```

---

## Part 3: Gold Layer (Advanced Analytics)

### Step 3.1: Create Dimension - `dim_suppliers.sql`

**Requirements:**

- Comprehensive supplier dimension with all attributes
- Include aggregated metrics from product_suppliers:
  - `total_products_supplied`
  - `avg_margin_percentage`
- Calculate supplier tier: 'platinum' (preferred + reliability > 0.9), 'gold' (preferred OR reliability > 0.85), 'silver' (others)
- Add `days_as_supplier` calculation

### Step 3.2: Create Fact Table - `fct_inventory_movements.sql`

**Requirements:**

- Transaction-level fact table
- Include supplier information for restocks
- Include product dimensions
- Calculate `inventory_value_change`
- Add transaction ranking per product (most recent = 1)

```sql
ROW_NUMBER() OVER (
    PARTITION BY product_id
    ORDER BY transaction_date DESC
) AS transaction_recency_rank
```

### Step 3.3: Create SCD Type 2 Model - `dim_inventory_history.sql`

This is the most challenging part! Implement a **Slowly Changing Dimension Type 2** to track inventory levels over time with validity periods.

**Requirements:**

- Track historical inventory states
- Add `valid_from` and `valid_to` dates
- Add `is_current` flag for the latest record
- Create a surrogate key combining `product_id` and `snapshot_date`

**SCD Type 2 Pattern:**

<details>
<summary>💡 SCD Type 2 Implementation</summary>

```sql
{{ config(materialized='table') }}

WITH snapshots AS (
    SELECT *
    FROM {{ ref('stg_inventory_snapshots') }}
),

with_validity AS (
    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['product_id', 'snapshot_date']) }} AS inventory_sk,

        product_id,
        snapshot_date,
        quantity_on_hand,
        quantity_available,
        inventory_health,

        -- SCD Type 2 validity
        snapshot_date AS valid_from,
        COALESCE(
            LEAD(snapshot_date) OVER (
                PARTITION BY product_id
                ORDER BY snapshot_date
            ) - INTERVAL '1 day',
            '9999-12-31'::DATE
        ) AS valid_to,

        -- Is this the current record?
        CASE
            WHEN LEAD(snapshot_date) OVER (
                PARTITION BY product_id
                ORDER BY snapshot_date
            ) IS NULL THEN TRUE
            ELSE FALSE
        END AS is_current

    FROM snapshots
)

SELECT * FROM with_validity
```

</details>

### Step 3.4: Create Report - `rpt_inventory_health.sql`

A comprehensive inventory health dashboard.

**Required Metrics:**

- Current inventory status per product
- Days of supply remaining
- Stockout risk score (based on velocity and current stock)
- Value at risk (inventory value for items below reorder point)
- Recommended reorder quantity

**Join these models:**

- `dim_inventory_history` (current records only)
- `stg_products`
- `dim_suppliers` (primary supplier)
- Sales velocity from `fct_order_items`

<details>
<summary>💡 Sales Velocity Calculation</summary>

```sql
-- Calculate average daily sales over last 30 days
WITH daily_sales AS (
    SELECT
        product_id,
        SUM(quantity) AS total_sold,
        COUNT(DISTINCT order_date) AS days_with_sales
    FROM {{ ref('fct_order_items') }} oi
    JOIN {{ ref('stg_orders') }} o ON oi.order_id = o.order_id
    WHERE o.order_date >= CURRENT_DATE - 30
    GROUP BY product_id
),

avg_daily_sales AS (
    SELECT
        product_id,
        total_sold / 30.0 AS avg_daily_units
    FROM daily_sales
)
```

</details>

### Step 3.5: Create Report - `rpt_supplier_performance.sql`

**Required Metrics per Supplier:**

- Total products supplied
- Total inventory value supplied
- Average margin on their products
- Number of stockouts for their products (supply chain issues indicator)
- On-time delivery score (use reliability_score as proxy)
- Rank suppliers by total value

**Include:**

- Window function for ranking
- Comparison to average supplier metrics

<details>
<summary>💡 Ranking and Comparison Pattern</summary>

```sql
-- Rank suppliers
RANK() OVER (ORDER BY total_inventory_value DESC) AS value_rank,

-- Compare to average
avg_margin - AVG(avg_margin) OVER () AS margin_vs_average
```

</details>

### Step 3.6: Create Report - `rpt_inventory_anomalies.sql`

Identify unusual inventory movements that may need investigation.

**Anomaly Detection Rules:**

1. Large adjustments (>10% of typical inventory)
2. Rapid inventory depletion (>50% drop in a week)
3. Negative inventory (data quality issue)
4. Unusual patterns (weekend transactions, holiday activity)
5. Cost anomalies (unit cost significantly different from average)

<details>
<summary>💡 Anomaly Detection Pattern</summary>

```sql
WITH stats AS (
    SELECT
        product_id,
        AVG(ABS(quantity)) AS avg_transaction_size,
        STDDEV(ABS(quantity)) AS stddev_transaction_size
    FROM {{ ref('stg_inventory_transactions') }}
    GROUP BY product_id
),

anomalies AS (
    SELECT
        t.*,
        s.avg_transaction_size,
        s.stddev_transaction_size,

        -- Flag if transaction is > 3 standard deviations from mean
        CASE
            WHEN ABS(t.quantity) > s.avg_transaction_size + (3 * s.stddev_transaction_size)
            THEN 'Large Transaction Anomaly'
            ELSE NULL
        END AS anomaly_type

    FROM {{ ref('stg_inventory_transactions') }} t
    JOIN stats s ON t.product_id = s.product_id
)

SELECT *
FROM anomalies
WHERE anomaly_type IS NOT NULL
```

</details>

---

## Part 4: Advanced Features

### Step 4.1: Create a Custom Macro

Create `macros/calculate_days_of_supply.sql`:

```sql
{% macro calculate_days_of_supply(inventory_qty, avg_daily_sales) %}
    CASE
        WHEN {{ avg_daily_sales }} > 0 THEN
            ROUND({{ inventory_qty }} / {{ avg_daily_sales }}, 1)
        ELSE NULL
    END
{% endmacro %}
```

Use this macro in your inventory reports.

### Step 4.2: Create an Incremental Model

Convert `fct_inventory_movements.sql` to an incremental model for efficient processing:

<details>
<summary>💡 Incremental Model Pattern</summary>

```sql
{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge'
) }}

SELECT
    transaction_id,
    product_id,
    -- other columns...
    transaction_date
FROM {{ ref('stg_inventory_transactions') }}

{% if is_incremental() %}
    WHERE transaction_date > (SELECT MAX(transaction_date) FROM {{ this }})
{% endif %}
```

</details>

### Step 4.3: Add Model Documentation

Create comprehensive documentation using doc blocks:

Create `models/gold/_inventory_docs.md`:

```markdown
{% docs inventory_health_status %}

Inventory health classification based on stock levels:

| Status      | Condition                           |
| ----------- | ----------------------------------- |
| critical    | Stockout - zero available inventory |
| low         | Below reorder point                 |
| healthy     | Above reorder point but below 3x    |
| overstocked | More than 3x reorder point          |

{% enddocs %}
```

Reference in your schema YAML:

```yaml
columns:
  - name: inventory_health
    description: '{{ doc("inventory_health_status") }}'
```

---

## Part 5: Validation & Analysis

### Step 5.1: Build Everything

```bash
# Build the complete pipeline
dbt build --select +rpt_inventory_health +rpt_supplier_performance +rpt_inventory_anomalies

# Run all tests
dbt test
```

### Step 5.2: Generate Documentation

```bash
dbt docs generate
dbt docs serve
```

**Verify:**

- Complex lineage graph is visible
- Doc blocks render correctly
- All tests are documented

### Step 5.3: Analytical Queries

Run these queries to validate your work:

```sql
-- 1. What products are at risk of stockout?
SELECT *
FROM gold.rpt_inventory_health
WHERE days_of_supply < 7
ORDER BY days_of_supply ASC;

-- 2. Which suppliers have the best margins?
SELECT *
FROM gold.rpt_supplier_performance
ORDER BY avg_margin_percentage DESC;

-- 3. What inventory anomalies need investigation?
SELECT *
FROM gold.rpt_inventory_anomalies
WHERE anomaly_type IS NOT NULL
ORDER BY transaction_date DESC;

-- 4. Historical inventory for a specific product (SCD Type 2)
SELECT *
FROM gold.dim_inventory_history
WHERE product_id = 1
ORDER BY valid_from;

-- 5. Point-in-time inventory lookup
SELECT *
FROM gold.dim_inventory_history
WHERE product_id = 1
  AND '2023-08-15' BETWEEN valid_from AND valid_to;
```

---

## ✅ Completion Checklist

### Bronze Layer

- [ ] `seeds/suppliers.csv` - 6+ suppliers with varying attributes
- [ ] `seeds/product_suppliers.csv` - Multiple suppliers per product, one primary each
- [ ] `seeds/inventory_transactions.csv` - 50+ transactions over 6 months
- [ ] `seeds/inventory_snapshots.csv` - Weekly snapshots for 3+ months
- [ ] All sources defined in `sources.yml` with descriptions

### Silver Layer

- [ ] `stg_suppliers.sql` with lead time categorization and contract status
- [ ] `stg_product_suppliers.sql` with margin calculations
- [ ] `stg_inventory_transactions.sql` with running totals
- [ ] `stg_inventory_snapshots.sql` with WoW changes and health status
- [ ] All staging tests pass including custom tests

### Gold Layer

- [ ] `dim_suppliers.sql` with aggregated metrics and tiering
- [ ] `fct_inventory_movements.sql` (incremental model)
- [ ] `dim_inventory_history.sql` implementing SCD Type 2
- [ ] `rpt_inventory_health.sql` with stockout risk scoring
- [ ] `rpt_supplier_performance.sql` with rankings
- [ ] `rpt_inventory_anomalies.sql` with detection rules

### Advanced Features

- [ ] Custom macro `calculate_days_of_supply` created and used
- [ ] At least one incremental model implemented
- [ ] Custom generic test for single primary supplier
- [ ] Doc blocks created and referenced

### Final Validation

- [ ] `dbt build` completes with no errors
- [ ] All tests pass
- [ ] Documentation generated with complete lineage
- [ ] Analytical queries return meaningful results

---

## 🚀 Bonus Challenges

1. **ABC Analysis:** Classify inventory as A (top 20% of value), B (next 30%), C (bottom 50%)

2. **Economic Order Quantity (EOQ):** Calculate optimal reorder quantities using the EOQ formula

3. **Seasonality Detection:** Identify products with seasonal demand patterns

4. **Supplier Diversification Risk:** Flag products dependent on single supplier

5. **Inventory Forecasting:** Create a simple moving average forecast model

6. **Audit Trail:** Create a model that tracks all changes to inventory-related models

---

## 🆘 Troubleshooting

### Window Function Errors

- Ensure `PARTITION BY` and `ORDER BY` columns exist
- Check for NULL values in ordering columns
- Use `ROWS BETWEEN` for running totals to avoid frame issues

### SCD Type 2 Issues

- Verify `valid_from` is always before `valid_to`
- Check that `is_current = TRUE` exists for each product
- Ensure surrogate keys are truly unique

### Incremental Model Not Working

- Verify `unique_key` column exists and is unique
- Check the `WHERE` clause in `{% if is_incremental() %}`
- Run with `--full-refresh` to rebuild completely

### Custom Test Failing

- Check test SQL returns rows that FAIL the test
- Verify the test is registered in the right location
- Use `dbt test --select test_name` to debug

### Performance Issues

- Consider adding indexes on frequently joined columns
- Review window function frame specifications
- Use incremental models for large transaction tables

### Circular Dependencies

- Ensure facts reference dimensions, not vice versa
- Reports should only reference facts/dims, not other reports
- Check `{{ ref() }}` calls form a valid DAG

---

## 📚 Additional Resources

- [dbt Incremental Models](https://docs.getdbt.com/docs/build/incremental-models)
- [Snowflake Window Functions](https://docs.snowflake.com/en/sql-reference/functions-analytic)
- [SCD Type 2 Pattern](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-2/)
- [dbt_utils Package](https://github.com/dbt-labs/dbt-utils)

---

Good luck! This exercise covers advanced patterns you'll encounter in production dbt projects. Take your time and build incrementally. 🚀
