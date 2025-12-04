# 🎓 dbt Hands-On Exercises

Welcome! These exercises will teach you the complete dbt workflow using the **Medallion Architecture** (Bronze → Silver → Gold).

## 📋 Prerequisites

Before starting, make sure you have:

- [ ] Completed the main project setup (see [QUICKSTART.md](../QUICKSTART.md))
- [ ] Successfully run `dbt build` on the existing project
- [ ] Access to Snowflake to verify your results
- [ ] Basic understanding of SQL

## 🗂️ Available Exercises

| Exercise                                                               | Difficulty        | Time Estimate | Description                                                         |
| ---------------------------------------------------------------------- | ----------------- | ------------- | ------------------------------------------------------------------- |
| [Exercise 1: Product Categories](./exercise_1_product_categories.md)   | ⭐ Beginner       | 45-60 min     | Add category data and create a sales-by-category report             |
| [Exercise 4: Customer Reviews](./exercise_4_customer_reviews.md)       | ⭐⭐ Intermediate | 60-90 min     | Build a complete review analytics pipeline with advanced testing    |
| [Exercise 5: Inventory Analytics](./exercise_5_inventory_analytics.md) | ⭐⭐⭐ Advanced   | 3-4 hours     | SCD Type 2, window functions, incremental models, anomaly detection |

## 🚀 Getting Started

1. **Pick an exercise** - Start with Exercise 1 if you're new to dbt
2. **Read the scenario** - Understand the business problem you're solving
3. **Work through each part** - Bronze → Silver → Gold, in order
4. **Use hints sparingly** - Try to solve it yourself first!
5. **Check your work** - Use the completion checklist at the end

## 💡 Tips for Success

- **Don't skip steps** - Each layer builds on the previous one
- **Run frequently** - Use `dbt run` and `dbt test` after each model
- **Read error messages** - They usually tell you exactly what's wrong
- **Check existing code** - Look at the existing models for patterns to follow
- **Use the docs** - Run `dbt docs serve` to explore the project structure

## 📚 Key Concepts Review

```
┌─────────────────────────────────────────────────────────────┐
│                    MEDALLION ARCHITECTURE                    │
├─────────────┬─────────────────┬─────────────────────────────┤
│   BRONZE    │     SILVER      │            GOLD             │
│   (Raw)     │   (Staging)     │    (Business-Ready)         │
├─────────────┼─────────────────┼─────────────────────────────┤
│ seeds/*.csv │ stg_*.sql       │ dim_*.sql, fct_*.sql        │
│ sources.yml │ {{ source() }} │ {{ ref() }}                 │
│ Schema: raw │ Schema: silver  │ Schema: gold                │
└─────────────┴─────────────────┴─────────────────────────────┘
```

## 🔧 Helpful Commands

```bash
# Load seed data
dbt seed

# Run a specific model
dbt run --select model_name

# Run a model and all its dependencies
dbt run --select +model_name

# Test a specific model
dbt test --select model_name

# Build everything (seed + run + test)
dbt build

# Generate and view documentation
dbt docs generate && dbt docs serve
```

## ❓ Need Help?

1. **Check the Troubleshooting section** at the end of each exercise
2. **Review existing models** in `models/silver/` and `models/gold/` for examples
3. **Expand the hints** in the exercise (click the ► arrows)
4. **Ask your instructor** or teammates

---

Good luck and have fun! 🚀
