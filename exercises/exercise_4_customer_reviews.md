# Exercise 4: Customer Reviews Analytics

## 🎯 Learning Objectives

By completing this exercise, you will learn how to:

- Design and create seed data with multiple relationships
- Build staging models with data validation logic
- Use dbt_utils package for advanced testing
- Create fact tables for transactional/event data
- Build aggregate reports with multiple metrics
- Implement the complete Bronze → Silver → Gold pipeline

---

## 📋 Scenario

The product team wants to understand customer satisfaction. They need:

1. A way to store and analyze product reviews
2. Average ratings per product
3. Identification of highly-rated and poorly-rated products
4. Customer review activity metrics

Your task is to build the complete data pipeline from raw review data to actionable reports.

---

## Part 1: Bronze Layer (Raw Data)

### Step 1.1: Create the Seed File

Create a new file called `reviews.csv` in the `seeds/` folder.

**Requirements:**

- Include these columns: `review_id`, `product_id`, `customer_id`, `rating`, `review_title`, `review_text`, `review_date`
- `rating` should be an integer from 1 to 5
- Create at least 15-20 reviews to have meaningful aggregations
- Include a mix of ratings (some 5-star, some 1-star, etc.)
- Use `product_id` values that exist in your `products.csv`
- Use `customer_id` values that exist in your `customers.csv`

**Hints:**

- Check `products.csv` and `customers.csv` for valid IDs to reference
- Spread reviews across different products and customers
- Include some products with multiple reviews, some with few
- Use realistic review dates (within the last year or two)

<details>
<summary>💡 Example Data Structure (click to expand if stuck)</summary>

```
review_id,product_id,customer_id,rating,review_title,review_text,review_date
1,101,1,5,Great product!,Exceeded my expectations. Would buy again.,2024-03-15
2,101,2,4,Good value,Nice quality for the price.,2024-03-20
3,102,1,2,Disappointed,Did not work as advertised.,2024-04-01
...
```

</details>

**Think about:**

- What makes a realistic distribution of ratings?
- Should every customer review every product? (No!)
- How do you handle text with commas in CSV? (Wrap in quotes)

### Step 1.2: Load the Seed Data

```bash
dbt seed --select reviews
```

Verify the data loaded correctly by checking the output.

### Step 1.3: Define the Source

Open `models/bronze/sources.yml` and add your `reviews` table.

**Requirements:**

- Add under the existing `raw` source
- Include a description for the table
- Document all columns with descriptions
- Consider adding `freshness` configuration (optional but good practice)

**Hints:**

- Follow the pattern of existing source definitions
- Think about what each column represents from a business perspective

---

## Part 2: Silver Layer (Staging)

### Step 2.1: Create the Staging Model

Create `stg_reviews.sql` in `models/silver/`.

**Requirements:**

- Select from `{{ source('raw', 'reviews') }}`
- Configure as a `view`
- Cast `review_date` to a proper DATE type
- Ensure `rating` is an INTEGER
- Trim whitespace from text fields
- Add a calculated field: `is_positive_review` (TRUE if rating >= 4)
- Add a calculated field: `review_length` (character count of review_text)

**Hints:**

- Use `CAST()` or `::date` for date conversion
- Use `TRIM()` for strings
- Use `LENGTH()` or `LEN()` for character count
- Use `CASE WHEN` or boolean expression for is_positive_review

<details>
<summary>💡 Calculated Field Hint (click to expand if stuck)</summary>

```sql
-- Boolean calculated field
rating >= 4 AS is_positive_review,

-- Or using CASE for more control
CASE
    WHEN rating >= 4 THEN TRUE
    ELSE FALSE
END AS is_positive_review,

-- String length
LENGTH(TRIM(review_text)) AS review_length
```

</details>

### Step 2.2: Add Schema Tests

Open `models/silver/_silver_schema.yml` and add comprehensive tests.

**Required Tests:**

- `unique` on `review_id`
- `not_null` on `review_id`, `product_id`, `customer_id`, `rating`, `review_date`
- `accepted_values` on `rating` for values [1, 2, 3, 4, 5]
- `relationships` test to verify `product_id` exists in products
- `relationships` test to verify `customer_id` exists in customers

**Hints:**

- Look at the dbt documentation for test syntax
- The `relationships` test ensures referential integrity
- You can use `dbt_utils.accepted_range` as an alternative to `accepted_values`

<details>
<summary>💡 Relationships Test Syntax (click to expand if stuck)</summary>

```yaml
columns:
  - name: product_id
    tests:
      - not_null
      - relationships:
          to: ref('stg_products')
          field: product_id
```

</details>

<details>
<summary>💡 Accepted Range Test (using dbt_utils)</summary>

```yaml
columns:
  - name: rating
    tests:
      - dbt_utils.accepted_range:
          min_value: 1
          max_value: 5
```

</details>

### Step 2.3: Run and Validate

```bash
# Build the model
dbt run --select stg_reviews

# Run all tests
dbt test --select stg_reviews
```

**Important:** Fix any test failures before proceeding. Common issues:

- Invalid product_id or customer_id references
- Rating values outside 1-5 range
- Missing required fields

---

## Part 3: Gold Layer (Business Logic)

### Step 3.1: Create the Fact Table

Create `fct_reviews.sql` in `models/gold/`.

**Requirements:**

- Reference staging model using `{{ ref('stg_reviews') }}`
- Configure as a `table`
- Include all relevant columns from staging
- Add `review_age_days` - days since the review was posted
- Add `rating_category` - 'Poor' (1-2), 'Average' (3), 'Good' (4-5)

**Hints:**

- Use `DATEDIFF()` or `CURRENT_DATE - review_date` for age calculation
- Use `CASE WHEN` for rating_category

<details>
<summary>💡 Date Calculation Hint (click to expand if stuck)</summary>

```sql
-- Snowflake syntax for days since review
DATEDIFF('day', review_date, CURRENT_DATE) AS review_age_days
```

</details>

### Step 3.2: Create Product Ratings Report

Create `rpt_product_ratings.sql` in `models/gold/`.

**Requirements:**

- Join reviews with product information
- Calculate per-product metrics:
  - `total_reviews` - count of reviews
  - `avg_rating` - average rating (rounded to 2 decimals)
  - `min_rating` - lowest rating received
  - `max_rating` - highest rating received
  - `positive_review_count` - count where rating >= 4
  - `positive_review_pct` - percentage of positive reviews
- Include product name and other relevant product attributes
- Order by average rating descending

**Models to Join:**

- `{{ ref('fct_reviews') }}` - your review fact table
- `{{ ref('stg_products') }}` or existing product dimension

**Hints:**

- Use `ROUND(AVG(rating), 2)` for clean decimal places
- Calculate percentage as: `positive_count * 100.0 / total_count`
- Handle division by zero with `NULLIF()` or `CASE WHEN`

<details>
<summary>💡 Aggregation Pattern (click to expand if stuck)</summary>

```sql
SELECT
    p.product_id,
    p.product_name,
    COUNT(*) AS total_reviews,
    ROUND(AVG(r.rating), 2) AS avg_rating,
    SUM(CASE WHEN r.rating >= 4 THEN 1 ELSE 0 END) AS positive_review_count,
    ROUND(
        SUM(CASE WHEN r.rating >= 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        1
    ) AS positive_review_pct
FROM {{ ref('fct_reviews') }} r
LEFT JOIN {{ ref('stg_products') }} p ON r.product_id = p.product_id
GROUP BY p.product_id, p.product_name
```

</details>

### Step 3.3: Create Customer Review Activity Report

Create `rpt_customer_review_activity.sql` in `models/gold/`.

**Requirements:**

- Aggregate review activity per customer
- Calculate:
  - `total_reviews_written` - how many reviews this customer wrote
  - `avg_rating_given` - average rating this customer gives
  - `first_review_date` - when they wrote their first review
  - `last_review_date` - when they wrote their most recent review
  - `days_since_last_review` - engagement recency
- Include customer name/email from customer data
- Identify "harsh critics" (avg rating < 3) vs "enthusiastic reviewers" (avg rating >= 4)

**Models to Join:**

- `{{ ref('fct_reviews') }}`
- `{{ ref('stg_customers') }}` or `{{ ref('dim_customers') }}`

<details>
<summary>💡 Reviewer Type Classification (click to expand if stuck)</summary>

```sql
CASE
    WHEN AVG(r.rating) < 3 THEN 'Harsh Critic'
    WHEN AVG(r.rating) >= 4 THEN 'Enthusiastic Reviewer'
    ELSE 'Balanced Reviewer'
END AS reviewer_type
```

</details>

### Step 3.4: Add Gold Layer Schema

Update `models/gold/_gold_schema.yml` with your new models.

**Requirements:**

- Add all three new models: `fct_reviews`, `rpt_product_ratings`, `rpt_customer_review_activity`
- Include descriptions for each model
- Document key columns
- Add appropriate tests (unique keys, not_null on important fields)

---

## Part 4: Validation

### Step 4.1: Build the Complete Pipeline

```bash
# Build reviews and all downstream models
dbt build --select +rpt_product_ratings +rpt_customer_review_activity

# Or build everything
dbt build
```

### Step 4.2: Verify Test Coverage

```bash
# Run all tests
dbt test

# Check test coverage for your new models
dbt test --select tag:reviews  # if you added tags
```

### Step 4.3: Generate and Review Documentation

```bash
dbt docs generate
dbt docs serve
```

**Check:**

- Can you see the lineage from `reviews` seed → `stg_reviews` → `fct_reviews` → reports?
- Are all your descriptions showing up?
- Do the test results appear?

### Step 4.4: Query Your Results

Connect to Snowflake and explore your data:

```sql
-- Top rated products
SELECT * FROM gold.rpt_product_ratings
ORDER BY avg_rating DESC
LIMIT 10;

-- Products needing attention (low ratings)
SELECT * FROM gold.rpt_product_ratings
WHERE avg_rating < 3;

-- Most active reviewers
SELECT * FROM gold.rpt_customer_review_activity
ORDER BY total_reviews_written DESC;

-- Find the harsh critics
SELECT * FROM gold.rpt_customer_review_activity
WHERE reviewer_type = 'Harsh Critic';
```

---

## ✅ Completion Checklist

- [ ] `seeds/reviews.csv` exists with 15+ realistic reviews
- [ ] Reviews reference valid product_id and customer_id values
- [ ] Source is defined in `models/bronze/sources.yml`
- [ ] `models/silver/stg_reviews.sql` builds successfully
- [ ] Staging model includes calculated fields (is_positive_review, review_length)
- [ ] All staging tests pass (including relationships tests)
- [ ] `models/gold/fct_reviews.sql` builds successfully
- [ ] `models/gold/rpt_product_ratings.sql` builds successfully
- [ ] `models/gold/rpt_customer_review_activity.sql` builds successfully
- [ ] Gold schema documentation is complete
- [ ] `dbt build` completes with no errors
- [ ] Documentation is generated and lineage is visible
- [ ] You can query reports and get meaningful insights

---

## 🚀 Bonus Challenges

1. **Sentiment Indicator:** Add logic to flag reviews with certain keywords as "needs_response" (e.g., "broken", "refund", "terrible")

2. **Review Velocity:** Create a model showing review count by week/month to see trends

3. **Rating Distribution:** Create `rpt_rating_distribution.sql` showing count of reviews at each rating level (1-5)

4. **Verified Purchases:** Add a `is_verified_purchase` column to reviews (customer actually bought the product) by joining with orders

5. **Advanced Testing:** Implement a custom test that ensures no customer reviews the same product twice

<details>
<summary>💡 Custom Test Hint for #5</summary>

Create `tests/generic/test_unique_combination.sql` or use `dbt_utils.unique_combination_of_columns`:

```yaml
models:
  - name: stg_reviews
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - product_id
            - customer_id
```

</details>

---

## 🆘 Troubleshooting

### Relationships test failing

- Check that the IDs in your reviews.csv actually exist in products.csv and customers.csv
- Run `SELECT DISTINCT product_id FROM raw.reviews` and compare with `SELECT product_id FROM raw.products`

### "Ambiguous column name" error

- When joining tables, always prefix columns with table alias: `r.rating` not just `rating`

### Aggregation errors

- Make sure all non-aggregated columns are in the GROUP BY clause
- Use table aliases consistently

### Division by zero

- Wrap denominators in `NULLIF(column, 0)` to return NULL instead of error
- Example: `positive_count / NULLIF(total_count, 0)`

### Circular dependency error

- Make sure reports reference fact/dim tables, not the other way around
- Check your `{{ ref() }}` calls form a DAG (directed acyclic graph)
