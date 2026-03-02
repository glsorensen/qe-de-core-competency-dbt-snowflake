# Media Analytics Capstone: Implementation Guide

## 🎯 Project Overview

**Goal**: Build a production-ready analytics pipeline on Snowflake (hosted on AWS) with dbt for media & entertainment analytics, demonstrating data engineering best practices, security, governance, and cost optimization.

**AWS Account**: 066396400174 (aws-innovationlabs-projectcatalyst)

## 📋 Implementation Roadmap

### Phase 1: AWS Infrastructure Setup (Week 1)

#### Day 1-2: S3 Setup
- [x] Design S3 bucket structure with Hive-style partitioning
- [ ] Create S3 bucket: `snowflake-media-analytics-066396400174`
- [ ] Configure lifecycle policies for cost optimization
- [ ] Enable versioning, encryption, and access logging
- [ ] Apply resource tags for cost tracking

**Commands**:
```bash
cd aws_s3_setup
chmod +x create_s3_bucket.sh
./create_s3_bucket.sh
```

#### Day 2-3: IAM Configuration
- [ ] Create IAM role for Snowflake integration
- [ ] Configure trust policy (requires Snowflake external ID)
- [ ] Attach read/write policies
- [ ] Test S3 access permissions

**Commands**:
```bash
# First, create storage integration in Snowflake (see below)
# Then update setup_iam_roles.sh with Snowflake details
chmod +x setup_iam_roles.sh
./setup_iam_roles.sh
```

#### Day 3: Sample Data Generation
- [ ] Install Python dependencies
- [ ] Generate sample media analytics data
- [ ] Upload to S3 with proper partitioning

**Commands**:
```bash
pip install pandas numpy pyarrow boto3
python generate_sample_data.py --upload-to-s3 --profile default
```

### Phase 2: Snowflake Configuration (Week 1-2)

#### Day 4-5: Snowflake Setup
- [ ] Create database: `MEDIA_ANALYTICS`
- [ ] Create schemas: `RAW`, `SILVER`, `GOLD`, `LOGS`
- [ ] Create storage integration
- [ ] Configure external stages
- [ ] Define file formats (Parquet, CSV, JSON)
- [ ] Create raw tables

**Snowflake Script**:
```bash
# Execute in Snowflake UI or via SnowSQL
cd snowflake_stages
# Run: 01_create_stages.sql
```

**Update IAM Trust Policy**:
After creating storage integration, run:
```sql
DESC STORAGE INTEGRATION s3_media_analytics_integration;
```
Copy `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` and update AWS IAM role trust policy.

#### Day 5-6: Data Ingestion
- [ ] Test COPY INTO commands
- [ ] Perform initial bulk load
- [ ] Set up incremental loading patterns
- [ ] Configure scheduled tasks
- [ ] Implement error handling

**Snowflake Script**:
```bash
# Run: 02_copy_commands.sql
```

#### Day 6-7: Continuous Ingestion (Optional but Recommended)
- [ ] Create SNS topic and SQS queue in AWS
- [ ] Configure S3 event notifications
- [ ] Create notification integration in Snowflake
- [ ] Set up Snowpipes for auto-ingestion
- [ ] Test end-to-end flow

**Snowflake Script**:
```bash
# Run: 03_snowpipe_setup.sql
```

### Phase 3: Dimensional Modeling (Week 2)

#### Design Star Schema
**Fact Tables**:
- `fct_viewership_events` - Viewership events with metrics
- `fct_revenue_transactions` - Revenue transactions

**Dimension Tables**:
- `dim_content` - Content catalog (movies, shows)
- `dim_users` - User profiles
- `dim_subscriptions` - Subscription tiers
- `dim_dates` - Date dimension
- `dim_devices` - Device types

**Snowflake Pattern** (Normalized Subdomain):
- `dim_content` → `dim_genres`, `dim_ratings`
- `dim_users` → `dim_countries`, `dim_age_groups`

#### Create dbt Models
- [ ] Design dimensional model ERD
- [ ] Document data contracts
- [ ] Create dbt staging models (Silver layer)
- [ ] Create dbt dimensional models (Gold layer)
- [ ] Implement surrogate keys
- [ ] Add slowly changing dimensions (SCD Type 2 if needed)

### Phase 4: dbt Project Enhancement (Week 2-3)

#### Day 8-10: Restructure dbt Project
- [ ] Reorganize folders: `staging/`, `intermediate/`, `marts/`
- [ ] Update `dbt_project.yml` with proper configurations
- [ ] Add model configs (materializations, tags, meta)
- [ ] Create sources.yml for raw tables
- [ ] Build ref() dependency graph
- [ ] Add macros for reusable logic

**Current Structure**:
```
models/
├── bronze/sources.yml
├── silver/stg_*.sql
└── gold/dim_*.sql, fct_*.sql, rpt_*.sql
```

**Target Structure**:
```
models/
├── staging/
│   ├── _sources.yml
│   ├── stg_viewership_events.sql
│   ├── stg_revenue_transactions.sql
│   └── ...
├── intermediate/
│   ├── int_user_metrics.sql
│   └── int_content_performance.sql
└── marts/
    ├── core/
    │   ├── dim_content.sql
    │   ├── dim_users.sql
    │   ├── dim_subscriptions.sql
    │   ├── fct_viewership_events.sql
    │   └── fct_revenue_transactions.sql
    └── analytics/
        ├── rpt_daily_viewership.sql
        ├── rpt_revenue_summary.sql
        └── rpt_user_engagement.sql
```

#### Day 10-12: Data Testing
- [ ] Add schema tests (not_null, unique)
- [ ] Add relationship tests (foreign keys)
- [ ] Add accepted_values tests
- [ ] Create custom SQL tests
- [ ] Implement freshness tests
- [ ] Add anomaly detection tests
- [ ] Create test matrix mapping to business risks

**Test Categories**:
1. **Schema Tests**: Data integrity (nulls, uniqueness)
2. **Relationship Tests**: Referential integrity
3. **Business Logic Tests**: Business rules validation
4. **Freshness Tests**: SLA compliance
5. **Anomaly Detection**: Statistical outliers

#### Day 12-13: Documentation
- [ ] Add model descriptions
- [ ] Document column definitions
- [ ] Create data lineage diagrams
- [ ] Write transformation logic docs
- [ ] Generate dbt docs

**Commands**:
```bash
dbt docs generate
dbt docs serve
```

### Phase 5: Observability with Elementary (Week 3)

#### Day 13-14: Elementary Setup
- [ ] Install Elementary dbt package
- [ ] Configure Elementary in `packages.yml`
- [ ] Run `dbt deps`
- [ ] Create `_logs` schema
- [ ] Run Elementary models
- [ ] Configure Elementary monitors
- [ ] Set up Elementary dashboard

**Configuration**:
```yaml
# packages.yml
packages:
  - package: elementary-data/elementary
    version: 0.15.0

# dbt_project.yml
models:
  elementary:
    +schema: logs
```

**Commands**:
```bash
dbt run --select elementary
edr monitor  # Generate observability report
```

#### Day 14-15: Monitoring & Alerting
- [ ] Configure anomaly detection monitors
- [ ] Set up data freshness monitors
- [ ] Create schema change alerts
- [ ] Implement test failure notifications
- [ ] Configure Slack/email alerts
- [ ] Build custom dashboards

### Phase 6: Security & Governance (Week 3-4)

#### Day 15-16: RBAC Implementation
- [ ] Define role hierarchy
- [ ] Create custom roles
- [ ] Grant database permissions
- [ ] Grant schema permissions
- [ ] Grant object permissions
- [ ] Assign roles to users

**Roles**:
- `DATA_ADMIN` - Full administrative access
- `DATA_ENGINEER` - Build and maintain pipelines
- `DATA_ANALYST` - Query and analyze data
- `DATA_CONSUMER` - Read-only access to marts
- `BI_USER` - Read-only for BI tools

**Snowflake Scripts**:
```bash
cd snowflake_rbac
# Execute 01_create_roles.sql through 05_assign_roles_to_users.sql
```

#### Day 16-17: Data Governance
- [ ] Implement object tagging (PII, sensitivity levels)
- [ ] Create masking policies for sensitive data
- [ ] Implement row-level security (if needed)
- [ ] Set up audit logging
- [ ] Create data lineage documentation
- [ ] Build RACI matrix

**Example Tagging**:
```sql
ALTER TABLE dim_users SET TAG pii = 'high';
ALTER TABLE dim_content SET TAG sensitivity = 'internal';
```

**Example Masking Policy**:
```sql
CREATE MASKING POLICY email_mask AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() IN ('DATA_ADMIN', 'DATA_ENGINEER') THEN val
    ELSE '***@***.com'
  END;

ALTER TABLE dim_users MODIFY COLUMN email 
  SET MASKING POLICY email_mask;
```

#### Day 17: Data Contracts & Lineage
- [ ] Document data contracts for each model
- [ ] Create data quality expectations
- [ ] Generate lineage graphs
- [ ] Maintain change log
- [ ] Document assumptions and decisions

### Phase 7: Cost Optimization (Week 4)

#### Day 18-19: Cost Monitoring
- [ ] Set up warehouse credit monitoring
- [ ] Track query performance metrics
- [ ] Analyze storage costs
- [ ] Monitor Snowpipe costs
- [ ] Create cost dashboards

**Monitoring Queries**:
```sql
-- Warehouse credit usage
SELECT
    warehouse_name,
    SUM(credits_used) AS total_credits,
    SUM(credits_used) * 4.00 AS estimated_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY warehouse_name
ORDER BY total_credits DESC;

-- Most expensive queries
SELECT
    query_id,
    query_text,
    total_elapsed_time/1000 AS exec_time_sec,
    credits_used_cloud_services,
    bytes_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY total_elapsed_time DESC
LIMIT 20;
```

#### Day 19-20: Optimization Implementation
- [ ] Right-size warehouses
- [ ] Configure auto-suspend (60 seconds)
- [ ] Implement query result caching strategy
- [ ] Add clustering keys to large tables
- [ ] Optimize materialization strategies
- [ ] Implement incremental models
- [ ] Archive old data

**Optimization Strategies**:
1. **Warehouse Sizing**: Start with X-SMALL, scale up based on need
2. **Auto-Suspend**: 60 seconds for dev, 300 seconds for prod
3. **Clustering**: Cluster by frequently filtered columns
4. **Materialization**: Views for dev, tables/incremental for prod
5. **Caching**: Leverage result cache for repeated queries

### Phase 8: Documentation & Demo (Week 4)

#### Day 20-21: Architecture Documentation
- [ ] Create architecture diagram (AWS + Snowflake + dbt)
- [ ] Write glossary of terms
- [ ] Document data flows
- [ ] Create runbooks for operations
- [ ] Write assumptions/decisions log (ADRs)

**Architecture Components**:
- S3 bucket with lifecycle policies
- IAM roles and policies
- Snowflake storage integration
- External stages and Snowpipes
- dbt models (Bronze → Silver → Gold)
- Elementary observability
- BI tool integration

#### Day 21-22: Demo Preparation
- [ ] Create demo notebook (Jupyter or Python script)
- [ ] Write end-to-end demo script
- [ ] Add sample queries
- [ ] Include data validation steps
- [ ] Document failure recovery procedures
- [ ] Create presentation slides

#### Day 22: AI Usage Documentation
- [ ] Document prompts used with Copilot
- [ ] Capture agent workflows
- [ ] Show before/after code diffs
- [ ] Document rejected suggestions with rationale
- [ ] Highlight AI-accelerated tasks

## 📊 Deliverables Checklist

### 1. Architecture Documentation
- [ ] Architecture diagram (Lucidchart/draw.io)
- [ ] Component narrative
- [ ] Decisions log (ADRs)
- [ ] Data flow diagrams

### 2. AWS Setup Notes
- [ ] S3 structure documentation
- [ ] IAM policies and roles
- [ ] Lifecycle policies
- [ ] Cost analysis

### 3. dbt Project
- [ ] Organized model structure
- [ ] Comprehensive tests
- [ ] Generated documentation
- [ ] Custom macros
- [ ] CI/CD configuration

### 4. Elementary Observability
- [ ] Elementary installation
- [ ] Monitors configured
- [ ] Dashboard screenshots
- [ ] Alert configurations

### 5. Security/Governance Pack
- [ ] RBAC scripts
- [ ] Masking policies
- [ ] Tagging strategy
- [ ] RACI matrix
- [ ] Audit procedures

### 6. Cost Maintenance Memo
- [ ] Current usage analysis
- [ ] Bottleneck identification
- [ ] Optimization recommendations
- [ ] Projected savings

### 7. Demo Runbook
- [ ] Setup instructions
- [ ] End-to-end execution steps
- [ ] Validation queries
- [ ] Failure recovery procedures

### 8. AI Usage Appendix
- [ ] Prompt examples
- [ ] Agent workflows
- [ ] Code improvements
- [ ] Lessons learned

## 🚀 Quick Start Commands

### Initial Setup
```bash
# 1. Set up AWS
cd aws_s3_setup
chmod +x *.sh
./create_s3_bucket.sh

# 2. Generate sample data
pip install -r requirements.txt
python generate_sample_data.py --upload-to-s3

# 3. Configure Snowflake (in Snowflake UI)
# Run snowflake_stages/01_create_stages.sql

# 4. Update IAM trust policy
# Get values from DESC STORAGE INTEGRATION
./setup_iam_roles.sh

# 5. Load data
# Run snowflake_stages/02_copy_commands.sql

# 6. Run dbt
dbt deps
dbt seed
dbt run
dbt test
dbt docs generate && dbt docs serve
```

## 📈 Success Metrics

1. **Data Pipeline**
   - ✅ 100% data loaded from S3
   - ✅ < 15 min data freshness
   - ✅ > 99% data quality tests passing

2. **Performance**
   - ✅ < 5 sec query response for dashboards
   - ✅ < 30 min for full dbt run
   - ✅ Efficient warehouse utilization (>60%)

3. **Cost**
   - ✅ < $100/month Snowflake credits
   - ✅ < $10/month S3 costs
   - ✅ Warehouse auto-suspend enabled

4. **Security**
   - ✅ RBAC implemented
   - ✅ PII masked for non-admin roles
   - ✅ Audit logging enabled

5. **Documentation**
   - ✅ 100% models documented
   - ✅ Architecture diagram complete
   - ✅ Runbooks tested

## 🆘 Troubleshooting

### S3 Access Issues
```bash
# Verify IAM role
aws iam get-role --role-name snowflake-s3-integration-role

# Test S3 access
aws s3 ls s3://snowflake-media-analytics-066396400174/raw/
```

### Snowflake Connection Issues
```bash
# Test SnowSQL connection
snowsql -a <account> -u <username>

# Verify storage integration
DESC STORAGE INTEGRATION s3_media_analytics_integration;
```

### dbt Issues
```bash
# Debug connection
dbt debug

# Run specific model
dbt run --select model_name

# Full refresh
dbt run --full-refresh
```

## 📚 Resources

- [AWS S3 Documentation](https://docs.aws.amazon.com/s3/)
- [Snowflake Documentation](https://docs.snowflake.com/)
- [dbt Documentation](https://docs.getdbt.com/)
- [Elementary Documentation](https://docs.elementary-data.com/)

## 🎓 Learning Objectives Met

- ✅ AWS S3 to Snowflake ingestion patterns
- ✅ Dimensional modeling (star & snowflake schemas)
- ✅ dbt best practices (staging → intermediate → marts)
- ✅ Comprehensive data testing
- ✅ Observability with Elementary
- ✅ Security & governance (RBAC, masking, tagging)
- ✅ Cost monitoring & optimization
- ✅ AI-assisted development workflows

---

**Next Step**: Start with Phase 1, Day 1 - Run `./create_s3_bucket.sh` to create your S3 infrastructure!
