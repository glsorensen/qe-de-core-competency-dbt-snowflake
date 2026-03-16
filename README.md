# 🏔️ dbt + Snowflake: Complete Medallion Architecture Guide

**A production-ready tutorial project demonstrating enterprise-grade dbt with Snowflake, the Medallion Architecture pattern, comprehensive data quality testing, and audit trail tracking.**

**Version**: 1.0.0 | **dbt**: 1.11.2 | **Snowflake Adapter**: 1.11.1 | **Python**: 3.12

---

## 📚 Table of Contents

1. [What You'll Learn](#what-youll-learn)
2. [Project Structure](#project-structure)
3. [Prerequisites & Setup](#prerequisites--setup)
4. [Testing Framework](#testing-framework)
5. [Running dbt](#running-dbt)
6. [dbt Commands Reference](#dbt-commands-reference)
7. [Documentation & Observability](#documentation--observability)
8. [Best Practices](#best-practices)
9. [Troubleshooting](#troubleshooting)

---

## 📚 What You'll Learn

- dbt fundamentals: Sources, models, refs, tests, documentation
- Medallion Architecture: Bronze → Silver → Gold pattern
- Data transformations and quality testing
- Query tagging and audit trail tracking
- Snowflake integration and optimization

---

> 📖 **For detailed architecture information, see [ARCHITECTURE.md](ARCHITECTURE.md)** which covers:
> - Medallion Architecture layers (Bronze → Silver → Gold)
> - Complete data flow and integration patterns
> - Schema design and data models (bronze, silver, gold)
> - Technology stack and design decisions
> - Scalability and performance strategies

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

Every dbt execution is tagged with metadata for tracking and compliance:
- Query tags: `project:snowcommerce_analytics|env:dev`
- Execution hooks capture start/end timestamps

See [AUDIT_TRAIL_SETUP.md](AUDIT_TRAIL_SETUP.md) for detailed setup and monitoring.

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

**Project Documentation:**
- [ARCHITECTURE.md](ARCHITECTURE.md) - Complete system design, data models, and technical decisions
- [AUDIT_TRAIL_SETUP.md](AUDIT_TRAIL_SETUP.md) - Query tagging and audit trail configuration
- [QUICKSTART.md](QUICKSTART.md) - 5-minute setup guide
- [dbt Docs](https://docs.getdbt.com/) | [Snowflake Docs](https://docs.snowflake.com/)

---

## License

MIT License - Use for learning and teaching

---

**Last Updated:** March 16, 2026 | **dbt Version:** 1.11.2 | **Snowflake Adapter:** 1.11.1

For detailed audit trail setup: see [AUDIT_TRAIL_SETUP.md](AUDIT_TRAIL_SETUP.md)

For quick start: see [QUICKSTART.md](QUICKSTART.md)
