# dbt + Snowflake Tutorial: Copilot Instructions

This is a **beginner-friendly tutorial** demonstrating dbt with Snowflake using the Medallion Architecture.

## Quick Reference

### Medallion Architecture (Bronze → Silver → Gold)

| Layer      | Schema   | Materialization | Purpose                           |
| ---------- | -------- | --------------- | --------------------------------- |
| **Bronze** | `raw`    | Seeds/Sources   | Raw data (CSV files → Snowflake)  |
| **Silver** | `silver` | Views           | Cleaned staging models (`stg_*`)  |
| **Gold**   | `gold`   | Tables          | Business-ready (`dim_*`, `fct_*`) |

### Key dbt Concepts

```sql
-- Bronze: Reference raw source tables
SELECT * FROM {{ source('raw', 'customers') }}

-- Silver/Gold: Reference other dbt models
SELECT * FROM {{ ref('stg_customers') }}
```

### Common Commands

```bash
dbt seed      # Load CSV files to Snowflake
dbt run       # Build all models
dbt test      # Run data quality tests
dbt build     # seed + run + test
dbt docs generate && dbt docs serve  # View documentation
```

### Model Naming

- `stg_*` = Silver staging models (cleaned data)
- `dim_*` = Gold dimension tables (entities)
- `fct_*` = Gold fact tables (events/transactions)
- `rpt_*` = Gold report tables (aggregations)

### Project Structure

```
models/
├── bronze/sources.yml    # Define raw data sources
├── silver/stg_*.sql      # Clean & standardize
└── gold/dim_*, fct_*     # Business logic & aggregations
seeds/                    # Sample CSV data files
```

For detailed setup instructions, see `README.md`.
