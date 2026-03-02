# Media Analytics Pipeline - Demo Runbook

**Author**: Meagan Ahmed | **Date**: Feb 2026 | **Duration**: 15-20 minutes

---

## 🎯 Demo Overview

**What You'll Show**: Production-ready analytics pipeline with AWS S3 → Snowflake → dbt
- 33 dbt models (Bronze → Silver → Gold)
- 135 tests (100% passing)
- Elementary observability
- 72% faster development with AI

**Target Audience**: Technical stakeholders, data engineers, leadership

---

## ✅ Pre-Demo Checklist

**Quick Checks**:
```bash
pwd  # Should be in project directory
dbt debug  # Should show "All checks passed!"
dbt test  # Should show 135 PASS
```

**Pre-Load**:
- [ ] Snowflake UI logged in
- [ ] dbt docs ready: `python3 -m http.server 8080 --directory target &`
- [ ] Terminal font size readable
- [ ] GitHub repo open in browser

---

## 🎬 Demo Script (15-20 min)

### 1. Introduction (2 min)

**Show**: Project structure
```bash
tree -L 2 -I 'target|dbt_packages|logs|.git'
ls -lh *.md  # Show ARCHITECTURE.md, AI_USAGE_DOCUMENTATION.md
```

**Say**: 
- "Production-ready pipeline using Medallion Architecture"
- "33 models, 135 tests, full observability"
- "Built 72% faster using AI assistance"

---

### 2. Infrastructure (2 min)

**Show Snowflake UI** → `MEDIA_ANALYTICS` database

**Point Out**:
- **RAW** (Bronze): 15 tables, 325K+ records
- **SILVER** (Staging): 15 views (`stg_*`)
- **GOLD** (Analytics): 7 dims + 6 facts + 11 reports
- **LOGS** (Elementary): 31 observability models

**Say**: "Clear separation of concerns across schemas"

---

### 3. dbt Models (5 min)

**Show Code Examples**:
```bash
# Bronze - Source definitions
cat models/bronze/sources.yml | head -30

# Silver - Staging transformation
cat models/silver/stg_viewership.sql

# Gold - Dimension model
cat models/gold/dim_content.sql

# Gold - Fact model
cat models/gold/fct_viewership.sql
```

**Say**: 
- "Bronze defines raw sources with basic tests"
- "Silver cleans and standardizes"
- "Gold creates star schema for analytics"

---

### 4. Data Quality (2 min)

**Run Live**:
```bash
dbt test
# Expected: Done. PASS=135 WARN=0 ERROR=0
```

**Show**: `models/gold/_gold_schema.yml` test examples

**Say**: 
- "135 tests covering all layers"
- "Unique, not_null, relationships, accepted_values"
- "100% passing = production-ready"

---

### 5. Elementary Observability (2 min)

**Show Snowflake** → `LOGS` schema tables

**Query Live**:
```sql
USE MEDIA_ANALYTICS.LOGS;

-- Recent test results
SELECT test_name, status, detected_at
FROM elementary_test_results
ORDER BY detected_at DESC LIMIT 10;

-- Model run history
SELECT name, status, execution_time, rows_affected
FROM dbt_run_results
WHERE status = 'success'
ORDER BY execution_time DESC LIMIT 10;
```

**Say**: "Rich metadata for monitoring and alerting"

---

### 6. dbt Documentation (3 min)

**Open**: `http://localhost:8080`

**Demo Flow**:
1. **Project Overview** - Show model/test counts
2. **Lineage Graph** - Click `fct_viewership`, show dependencies
3. **Model Details** - Open `rpt_daily_content_performance`
   - Show description
   - Show columns with docs
   - Show compiled SQL
4. **Search** - Search "viewership"

**Say**: 
- "Interactive docs generated from code"
- "Self-service for analysts"
- "Auto-updates with changes"

---

### 7. Full Pipeline Run (3 min)

**Execute**:
```bash
dbt clean && dbt deps && dbt seed && dbt run && dbt test
```

**Expected Output**:
- seed: 15 PASS
- run: 33 PASS  
- test: 135 PASS

**Say**: 
- "Complete pipeline in <5 minutes"
- "Fully automated and reproducible"
- "Ready for CI/CD"

---

### 8. Business Value (2 min)

**Query in Snowflake**:
```sql
USE MEDIA_ANALYTICS.GOLD;

-- Top content by engagement
SELECT content_title, total_views, engagement_score, total_revenue
FROM rpt_daily_content_performance
WHERE report_date = CURRENT_DATE - 1
ORDER BY engagement_score DESC LIMIT 10;

-- High-value users at churn risk
SELECT user_id, total_revenue, total_watch_time_hours, is_at_risk
FROM rpt_user_engagement
WHERE is_at_risk = TRUE AND total_revenue > 100
ORDER BY total_revenue DESC;
```

**Say**: 
- "Business-ready reports for decision making"
- "Churn prediction, revenue optimization"
- "Complete lineage from source to insight"

---

### 9. AI Impact (1 min)

**Show**: `AI_USAGE_DOCUMENTATION.md` time savings table

**Highlight**:
- 53 hours manual → 15 hours with AI (72% faster)
- 90% of boilerplate AI-generated
- Human validation on all code

**Say**: "AI as productivity multiplier, not replacement"

---

## 🎯 Wrap-Up (1 min)

**Key Achievements**:
✅ Production-ready pipeline (33 models, 135 tests)  
✅ Best practices (modular code, comprehensive testing, self-documenting)  
✅ Business value (content analytics, user engagement, churn prediction)  
✅ AI-accelerated (72% time savings, professional quality)

**Next Steps** (Production):
- CI/CD with GitHub Actions
- Orchestration with dbt Cloud/Airflow
- Real-time ingestion with Snowpipe
- Advanced monitoring with Elementary CLI

---

## 🐛 Quick Troubleshooting

| Issue | Solution |
|-------|----------|
| `dbt debug` fails | Check `profiles.yml`, verify Snowflake credentials |
| Tests failing | Run `dbt test --debug`, check error messages |
| Docs won't load | Use existing: `python3 -m http.server 8080 --directory target` |
| Models not found | Run `dbt clean && dbt deps && dbt compile` |

---

## 📊 Quick Reference Data

| Layer | Schema | Objects | Records |
|-------|--------|---------|---------|
| Bronze | RAW | 15 tables | 325K+ |
| Silver | SILVER | 15 views | On-demand |
| Gold | GOLD | 24 tables | Optimized |
| Logs | LOGS | 31 models | Incremental |

---

## 💬 Anticipated Q&A

**Q: How long did this take?**  
A: 15 hours with AI (vs. 53 hours manual) = 72% faster

**Q: How do you handle incremental loads?**  
A: Currently full refresh via `dbt seed`. Production would use incremental models + Snowpipe

**Q: What about data quality SLAs?**  
A: 135 tests, Elementary tracks results over time, can set up alerts

**Q: How do you handle PII?**  
A: Architecture supports Snowflake masking policies and RBAC. Using sample data currently

**Q: What's the cost?**  
A: ~$30/mo Snowflake (60s auto-suspend) + ~$5/mo S3 for dev environment

**Q: How do you deploy to production?**  
A: GitHub Actions for CI/CD: test on PRs, deploy to dev/staging/prod sequentially

---

## 🎤 Demo Tips

✅ **Do**:
- Run commands live (shows confidence)
- Speak clearly and pace yourself
- Connect technical features to business value
- Show, don't just tell
- Leave time for Q&A

❌ **Don't**:
- Rush through sections
- Apologize for environment issues
- Get lost in technical weeds
- Forget to highlight AI usage
- Skip the business value section

---

**Good luck with your demo! 🚀**

*For full details, see ARCHITECTURE.md and AI_USAGE_DOCUMENTATION.md*
