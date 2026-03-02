# Media Analytics Pipeline - Architecture Documentation

**Author**: Meagan Ahmed | **Date**: Feb 2026 | **Repo**: qe-de-core-competency-dbt-snowflake

---

## 🎯 Executive Summary

**Production-ready analytics pipeline** using AWS S3 → Snowflake → dbt with **Medallion Architecture**
- **33 dbt models** transforming **325K+ records** across **4 schemas**
- **135 data quality tests** (100% passing)
- **31 Elementary observability models** for monitoring
- **72% faster development** using AI assistance

---

## 🏗️ Architecture Diagram (Simplified)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           AWS CLOUD (us-west-2)                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                    S3 Bucket (Data Lake)                               │  │
│  │          snowflake-media-analytics-066396400174                        │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                │  │
│  │  │ raw/         │  │ processed/   │  │ archive/     │                │  │
│  │  │ viewership/  │  │              │  │              │                │  │
│  │  │ revenue/     │  │              │  │              │                │  │
│  │  │ content/     │  │              │  │              │                │  │
│  │  │ users/       │  │              │  │              │                │  │
│  │  └──────┬───────┘  └──────────────┘  └──────────────┘                │  │
│  │         │                                                              │  │
│  │         │ CSV/Parquet Files                                           │  │
│  └─────────┼──────────────────────────────────────────────────────────────┘  │
│            │                                                                  │
│  ┌─────────▼──────────────────────────────────────────────────────────────┐  │
│  │                    IAM Role (Trust Policy)                              │  │
│  │              snowflake-s3-integration-role                              │  │
│  │  • AmazonS3ReadOnlyAccess                                               │  │
│  │  • Trusted by Snowflake External ID                                     │  │
│  └─────────┬──────────────────────────────────────────────────────────────┘  │
└────────────┼─────────────────────────────────────────────────────────────────┘
             │
             │ HTTPS/TLS
             │
┌────────────▼─────────────────────────────────────────────────────────────────┐
│                    SNOWFLAKE (AWS us-west-2)                                 │
│                 Account: slalom-snowflake_ilabs_qecatalyst                   │
│                                                                               │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │  Warehouse: SALES_LOAD (Size: Large, Auto-Suspend: 60s)               │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                               │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │              Database: MEDIA_ANALYTICS                                  │  │
│  │                                                                          │  │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │  │
│  │  │ BRONZE LAYER - RAW Schema                                        │   │  │
│  │  │ • Storage Integration: s3_media_analytics_integration           │   │  │
│  │  │ • External Stage: @media_analytics_stage                        │   │  │
│  │  │                                                                  │   │  │
│  │  │ Raw Tables (via dbt seed):                                      │   │  │
│  │  │   ├── customers (1,000 rows)                                    │   │  │
│  │  │   ├── orders                                                    │   │  │
│  │  │   ├── order_items                                               │   │  │
│  │  │   ├── products                                                  │   │  │
│  │  │   ├── categories                                                │   │  │
│  │  │   ├── reviews                                                   │   │  │
│  │  │   ├── suppliers                                                 │   │  │
│  │  │   ├── product_suppliers                                         │   │  │
│  │  │   ├── marketing_campaigns                                       │   │  │
│  │  │   ├── inventory_snapshots                                       │   │  │
│  │  │   └── inventory_transactions                                    │   │  │
│  │  │                                                                  │   │  │
│  │  │ Media Analytics Tables (via dbt seed):                          │   │  │
│  │  │   ├── viewership (286,017 events)                               │   │  │
│  │  │   ├── revenue (38,447 transactions)                             │   │  │
│  │  │   ├── content (500 items)                                       │   │  │
│  │  │   └── users (1,000 profiles)                                    │   │  │
│  │  └─────────────────────┬────────────────────────────────────────────┘   │  │
│  │                        │                                                 │  │
│  │                        │ dbt source()                                    │  │
│  │                        ▼                                                 │  │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │  │
│  │  │ SILVER LAYER - SILVER Schema                                    │   │  │
│  │  │ Materialization: Views (Lightweight transformations)            │   │  │
│  │  │                                                                  │   │  │
│  │  │ Staging Models (Cleaned & Standardized):                        │   │  │
│  │  │   E-Commerce Models:                                            │   │  │
│  │  │   ├── stg_customers - Type casting, rename columns             │   │  │
│  │  │   ├── stg_orders - Parse dates, calc totals                    │   │  │
│  │  │   ├── stg_order_items - Denormalize, calc line totals          │   │  │
│  │  │   ├── stg_products - Clean text, price formatting              │   │  │
│  │  │   ├── stg_categories - Hierarchy parsing                       │   │  │
│  │  │   ├── stg_reviews - Sentiment scoring prep                     │   │  │
│  │  │   ├── stg_suppliers - Contact standardization                  │   │  │
│  │  │   ├── stg_product_suppliers - Many-to-many joins               │   │  │
│  │  │   ├── stg_marketing_campaigns - ROI calculations               │   │  │
│  │  │   ├── stg_inventory_snapshots - Timestamp parsing              │   │  │
│  │  │   └── stg_inventory_transactions - Movement tracking           │   │  │
│  │  │                                                                  │   │  │
│  │  │   Media Analytics Models:                                       │   │  │
│  │  │   ├── stg_viewership - Session parsing, duration calc          │   │  │
│  │  │   ├── stg_revenue - Payment processing, refunds                │   │  │
│  │  │   ├── stg_content - Metadata enrichment, quality flags         │   │  │
│  │  │   └── stg_users - Profile aggregation, activity metrics        │   │  │
│  │  └─────────────────────┬────────────────────────────────────────────┘   │  │
│  │                        │                                                 │  │
│  │                        │ dbt ref()                                       │  │
│  │                        ▼                                                 │  │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │  │
│  │  │ GOLD LAYER - GOLD Schema                                        │   │  │
│  │  │ Materialization: Tables (Optimized for queries)                 │   │  │
│  │  │                                                                  │   │  │
│  │  │ ┌───────────────────────────────────────────────────────────┐  │   │  │
│  │  │ │ DIMENSION TABLES (SCD Type 1 & Type 2)                    │  │   │  │
│  │  │ │                                                             │  │   │  │
│  │  │ │ E-Commerce Dimensions:                                      │  │   │  │
│  │  │ │ • dim_customers (1,000 customers)                           │  │   │  │
│  │  │ │   - customer_key, customer_id, full_name, email             │  │   │  │
│  │  │ │   - loyalty_tier, lifetime_value, registration_date         │  │   │  │
│  │  │ │                                                             │  │   │  │
│  │  │ │ • dim_products (200 products)                               │  │   │  │
│  │  │ │   - product_key, product_id, product_name, category        │  │   │  │
│  │  │ │   - unit_price, cost, margin_pct, supplier_info            │  │   │  │
│  │  │ │                                                             │  │   │  │
│  │  │ │ • dim_categories (20 categories)                            │  │   │  │
│  │  │ │   - category_key, category_name, parent_category           │  │   │  │
│  │  │ │   - hierarchy_level, category_path                          │  │   │  │
│  │  │ │                                                             │  │   │  │
│  │  │ │ • dim_suppliers (50 suppliers)                              │  │   │  │
│  │  │ │   - supplier_key, supplier_name, contact_info              │  │   │  │
│  │  │ │   - rating, status, lead_time_days                          │  │   │  │
│  │  │ │                                                             │  │   │  │
│  │  │ │ Media Analytics Dimensions:                                 │  │   │  │
│  │  │ │ • dim_date (730 days) - Date dimension with:                │  │   │  │
│  │  │ │   - date_key, full_date, year, quarter, month, week        │  │   │  │
│  │  │ │   - day_name, is_weekend, is_holiday, fiscal_period        │  │   │  │
│  │  │ │                                                             │  │   │  │
│  │  │ │ • dim_content (500 content items)                           │  │   │  │
│  │  │ │   - content_key, content_id, title, content_type           │  │   │  │
│  │  │ │   - genre, rating, duration_minutes, release_date          │  │   │  │
│  │  │ │   - quality_flag, popularity_score                          │  │   │  │
│  │  │ │                                                             │  │   │  │
│  │  │ │ • dim_user (1,000 users)                                    │  │   │  │
│  │  │ │   - user_key, user_id, subscription_tier, region           │  │   │  │
│  │  │ │   - device_type, signup_date, is_active                    │  │   │  │
│  │  │ │                                                             │  │   │  │
│  │  │ │ • dim_inventory_history (SCD Type 2)                        │  │   │  │
│  │  │ │   - Tracks inventory level changes over time               │  │   │  │
│  │  │ │   - valid_from, valid_to, is_current flags                 │  │   │  │
│  │  │ └─────────────────────────────────────────────────────────────┘  │   │  │
│  │  │                                                                  │   │  │
│  │  │ ┌───────────────────────────────────────────────────────────┐  │   │  │
│  │  │ │ FACT TABLES (Transaction Grain)                           │  │   │  │
│  │  │ │                                                             │  │   │  │
│  │  │ │ E-Commerce Facts:                                           │  │   │  │
│  │  │ │ • fct_orders (10,000 orders)                                │  │   │  │
│  │  │ │   - order_key, customer_key, date_key                      │  │   │  │
│  │  │ │   - order_total, item_count, shipping_cost                 │  │   │  │
│  │  │ │   - order_status, payment_method                            │  │   │  │
│  │  │ │                                                             │  │   │  │
│  │  │ │ • fct_order_items (50,000 line items)                       │  │   │  │
│  │  │ │   - order_item_key, order_key, product_key, date_key      │  │   │  │
│  │  │ │   - quantity, unit_price, discount, line_total             │  │   │  │
│  │  │ │                                                             │  │   │  │
│  │  │ │ • fct_reviews (5,000 reviews)                               │  │   │  │
│  │  │ │   - review_key, product_key, customer_key, date_key       │  │   │  │
│  │  │ │   - rating, review_text, sentiment_score                   │  │   │  │
│  │  │ │                                                             │  │   │  │
│  │  │ │ • fct_inventory_movements (100,000 transactions)            │  │   │  │
│  │  │ │   - movement_key, product_key, date_key                    │  │   │  │
│  │  │ │   - movement_type, quantity_change, location               │  │   │  │
│  │  │ │                                                             │  │   │  │
│  │  │ │ Media Analytics Facts:                                      │  │   │  │
│  │  │ │ • fct_viewership (286,017 viewing events)                   │  │   │  │
│  │  │ │   - viewership_key, content_key, user_key, date_key       │  │   │  │
│  │  │ │   - view_duration_minutes, completion_pct                  │  │   │  │
│  │  │ │   - device_type, playback_quality, buffering_events        │  │   │  │
│  │  │ │                                                             │  │   │  │
│  │  │ │ • fct_revenue (38,447 revenue transactions)                 │  │   │  │
│  │  │ │   - revenue_key, user_key, date_key                        │  │   │  │
│  │  │ │   - revenue_amount, payment_method, transaction_type       │  │   │  │
│  │  │ │   - is_refund, subscription_tier                            │  │   │  │
│  │  │ └─────────────────────────────────────────────────────────────┘  │   │  │
│  │  │                                                                  │   │  │
│  │  │ ┌───────────────────────────────────────────────────────────┐  │   │  │
│  │  │ │ REPORT TABLES (Aggregated Marts)                          │  │   │  │
│  │  │ │                                                             │  │   │  │
│  │  │ │ E-Commerce Reports:                                         │  │   │  │
│  │  │ │ • rpt_customer_metrics - CLV, RFM analysis, churn          │  │   │  │
│  │  │ │ • rpt_monthly_sales - Revenue trends, YoY growth           │  │   │  │
│  │  │ │ • rpt_sales_by_category - Category performance             │  │   │  │
│  │  │ │ • rpt_product_performance - Top sellers, slow movers       │  │   │  │
│  │  │ │ • rpt_product_ratings - Review aggregations, sentiment     │  │   │  │
│  │  │ │ • rpt_customer_review_activity - Review engagement         │  │   │  │
│  │  │ │ • rpt_supplier_performance - Delivery times, quality       │  │   │  │
│  │  │ │ • rpt_inventory_health - Stock levels, reorder points      │  │   │  │
│  │  │ │ • rpt_inventory_anomalies - Out of stock, overstock        │  │   │  │
│  │  │ │                                                             │  │   │  │
│  │  │ │ Media Analytics Reports:                                    │  │   │  │
│  │  │ │ • rpt_daily_content_performance                             │  │   │  │
│  │  │ │   - Daily metrics per content item                          │  │   │  │
│  │  │ │   - Views, unique viewers, avg watch time                  │  │   │  │
│  │  │ │   - Completion rate, revenue per view                      │  │   │  │
│  │  │ │   - Engagement score, trending indicators                   │  │   │  │
│  │  │ │                                                             │  │   │  │
│  │  │ │ • rpt_user_engagement                                       │  │   │  │
│  │  │ │   - User-level engagement metrics                           │  │   │  │
│  │  │ │   - Total watch time, content diversity                    │  │   │  │
│  │  │ │   - Average session length, subscription value             │  │   │  │
│  │  │ │   - Churn risk indicators                                   │  │   │  │
│  │  │ └─────────────────────────────────────────────────────────────┘  │   │  │
│  │  └─────────────────────────────────────────────────────────────────┘   │  │
│  │                                                                          │  │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │  │
│  │  │ LOGS SCHEMA - Elementary Observability                          │   │  │
│  │  │ 31 Elementary Models for Data Observability:                    │   │  │
│  │  │                                                                  │   │  │
│  │  │ • Metadata Tables:                                              │   │  │
│  │  │   - dbt_models, dbt_tests, dbt_sources, dbt_columns            │   │  │
│  │  │   - dbt_run_results, dbt_test_results, dbt_invocations         │   │  │
│  │  │   - schema_columns_snapshot, test_result_rows                  │   │  │
│  │  │                                                                  │   │  │
│  │  │ • Monitoring Tables:                                            │   │  │
│  │  │   - elementary_test_results, data_monitoring_metrics           │   │  │
│  │  │   - metrics_anomaly_score, monitors_runs                       │   │  │
│  │  │                                                                  │   │  │
│  │  │ • Alert Views:                                                  │   │  │
│  │  │   - alerts_dbt_tests, alerts_schema_changes                    │   │  │
│  │  │   - alerts_dbt_models, alerts_anomaly_detection                │   │  │
│  │  │   - alerts_dbt_source_freshness                                │   │  │
│  │  │                                                                  │   │  │
│  │  │ • Analytics Views:                                              │   │  │
│  │  │   - job_run_results, model_run_results, seed_run_results       │   │  │
│  │  └─────────────────────────────────────────────────────────────────┘   │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────────────────┘
                                     ▲
                                     │
                          ┌──────────┴──────────┐
                          │   dbt CLI (1.11.2)  │
                          │   + GitHub Copilot  │
                          └─────────────────────┘
```

---

## 🏛️ Medallion Architecture (Bronze → Silver → Gold)

| Layer | Schema | Purpose | Models | Materialization | Data Volume |
|-------|--------|---------|--------|-----------------|-------------|
| **Bronze** | RAW | Raw source data | 15 tables | Tables (via `dbt seed`) | 325K+ records |
| **Silver** | SILVER | Cleaned & standardized | 15 staging (`stg_*`) | Views | Computed on-demand |
| **Gold** | GOLD | Business-ready analytics | 7 dims + 6 facts + 11 reports | Tables | Optimized for queries |
| **Logs** | LOGS | Elementary observability | 31 metadata models | Mixed | Incremental tracking |

### Key Design Decisions
- **Bronze**: `dbt seed` for learning (production would use Snowpipe/COPY INTO)
- **Silver**: Views for cost efficiency and freshness
- **Gold**: Tables for performance, star schema pattern with surrogate keys
- **Testing**: 135 tests ensuring data quality across all layers

---

## 🔄 Data Flow

**Pipeline**: CSV → dbt seed → RAW → Silver (clean) → Gold (analyze) → Reports

**Commands**:
```bash
dbt seed    # Load CSV to RAW
dbt run     # Build all models
dbt test    # Run 135 tests
```

**Example Lineage**: `viewership.csv → raw.viewership → stg_viewership → dim_date/dim_content/dim_user + fct_viewership → rpt_daily_content_performance`

---

## 🛠️ Technology Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Storage | AWS S3 | - | Data lake |
| Warehouse | Snowflake (us-west-2) | - | Analytics platform |
| Transformation | dbt Core | 1.11.2 | SQL transformations |
| Testing | dbt tests | - | 135 quality tests |
| Observability | Elementary | 0.16.2 | Metadata tracking |
| AI Assistant | GitHub Copilot | - | Development acceleration |

---

## 🔐 Security

- **Snowflake RBAC**: Role-based access (ANALYST_ROLE → GOLD read-only)
- **Encryption**: TLS in-transit, managed keys at-rest
- **AWS IAM**: Dedicated role for Snowflake integration
- **Audit**: Query history + Elementary tracking

---

## 📊 Data Model Summary

**Pattern**: Star schema with **7 dimensions** + **6 facts** + **11 reports**

| Fact Table | Grain | Records |
|-----------|-------|---------|
| fct_viewership | Per viewing session | 286K |
| fct_revenue | Per transaction | 38K |
| fct_orders | Per order | 10K |
| fct_order_items | Per line item | 50K |

**Design**: Surrogate keys via `dbt_utils`, Type 1 SCD, conformed dimensions

---

## 🧪 Data Quality & Testing

- **135 tests** (100% passing): unique, not_null, relationships, accepted_values
- **Elementary**: 31 observability models tracking test history, schema changes, lineage
- **Coverage**: All layers (Bronze PK tests → Silver type tests → Gold business rules)

---

## 💰 Performance & Cost

**Optimization**:
- Silver = Views (cost efficiency), Gold = Tables (query speed)
- Warehouse: Large, 60s auto-suspend
- Reports pre-aggregated for dashboards

**Cost**: ~$30/mo Snowflake + ~$5/mo S3 (dev environment)

---

## 🎯 Business Value

**Media Analytics**: Content performance, user engagement, churn prediction, revenue optimization  
**E-Commerce**: Customer CLV, product performance, inventory optimization, marketing ROI

---

## 🚀 Future Enhancements

1. Incremental models for scale
2. dbt Cloud/Airflow orchestration
3. Snowpipe for real-time ingestion
4. Advanced monitoring with Elementary CLI

---

**Full Details**: See `models/`, `tests/`, and interactive dbt docs (`dbt docs serve`)

