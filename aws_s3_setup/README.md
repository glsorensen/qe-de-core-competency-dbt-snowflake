# AWS S3 Setup for Snowflake Analytics Pipeline

## Overview
This document outlines the AWS S3 infrastructure for ingesting media & entertainment analytics data into Snowflake.

## AWS Account Details
- **Account ID**: 066396400174
- **Account Name**: aws-innovationlabs-projectcatalyst
- **Service Provider**: Amazon Web Services, Inc.

## S3 Bucket Structure

### Primary Data Bucket
**Bucket Name**: `s3://snowflake-media-analytics-meagan-${ACCOUNT_ID}/`

```
snowflake-media-analytics-meagan-066396400174/
├── raw/                          # Raw ingestion zone
│   ├── viewership/               # Viewership events
│   │   ├── year=2024/
│   │   │   ├── month=01/
│   │   │   │   ├── day=01/
│   │   │   │   │   └── viewership_20240101_*.parquet
│   │   │   │   └── day=02/
│   │   │   └── month=02/
│   │   └── year=2025/
│   ├── revenue/                  # Revenue transactions
│   │   ├── year=2024/
│   │   │   └── month=01/
│   │   │       └── revenue_20240101_*.parquet
│   ├── content/                  # Content metadata
│   │   └── content_catalog_*.parquet
│   ├── users/                    # User profiles
│   │   └── users_*.parquet
│   └── subscriptions/            # Subscription data
│       └── subscriptions_*.parquet
├── stage/                        # Staging/validation zone
│   └── [same structure as raw]
└── archive/                      # Archived/processed data
    └── [same structure as raw]
```

### File Format Standards

#### Parquet Files (Preferred)
- **Naming Convention**: `{entity}_{YYYYMMDD}_{batch_id}_{sequence}.parquet`
- **Compression**: Snappy
- **Partitioning**: By date (year/month/day)
- **Row Group Size**: 128MB
- **Benefits**: Columnar storage, efficient compression, schema evolution

#### CSV Files (Legacy/External Sources)
- **Naming Convention**: `{entity}_{YYYYMMDD}_{batch_id}.csv`
- **Delimiter**: Pipe (|) to handle commas in text
- **Encoding**: UTF-8
- **Header**: Include header row
- **Quote Character**: Double quotes

#### JSON Files (Event Streams)
- **Naming Convention**: `{entity}_{YYYYMMDD}_{batch_id}.json`
- **Format**: Line-delimited JSON (NDJSON)
- **Compression**: gzip

## S3 Lifecycle Policies

### Raw Data Retention
```yaml
Rule Name: transition-raw-to-ia
Status: Enabled
Scope: raw/
Transitions:
  - Days: 30
    Storage Class: STANDARD_IA
  - Days: 90
    Storage Class: GLACIER_IR
  - Days: 365
    Storage Class: DEEP_ARCHIVE
Expiration: Never (archive indefinitely)
```

### Stage Data Cleanup
```yaml
Rule Name: cleanup-staging
Status: Enabled
Scope: stage/
Expiration: 7 days
```

### Archive Management
```yaml
Rule Name: archive-deep-storage
Status: Enabled
Scope: archive/
Transitions:
  - Days: 1
    Storage Class: GLACIER_FLEXIBLE_RETRIEVAL
Expiration: 2555 days (7 years)
```

## IAM Configuration

### Snowflake Integration Role

**Role Name**: `snowflake-s3-integration-role`
**Trust Policy**: Allow Snowflake AWS account to assume role

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::<SNOWFLAKE_AWS_ACCOUNT>:user/snowflake"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<SNOWFLAKE_EXTERNAL_ID>"
        }
      }
    }
  ]
}
```

### IAM Policy - Read Access

**Policy Name**: `snowflake-s3-read-policy`

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::snowflake-media-analytics-meagan-066396400174",
        "arn:aws:s3:::snowflake-media-analytics-meagan-066396400174/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListAllMyBuckets"
      ],
      "Resource": "*"
    }
  ]
}
```

### IAM Policy - Write Access (For Error Files)

**Policy Name**: `snowflake-s3-write-policy`

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::snowflake-media-analytics-meagan-066396400174/errors/*",
        "arn:aws:s3:::snowflake-media-analytics-meagan-066396400174/rejected/*"
      ]
    }
  ]
}
```

## S3 Bucket Configuration

### Versioning
- **Status**: Enabled
- **Purpose**: Track data changes, support rollback, audit trail

### Encryption
- **Type**: SSE-S3 (Server-Side Encryption with Amazon S3-Managed Keys)
- **Alternative**: SSE-KMS for enhanced control
- **Bucket Key**: Enabled (reduce KMS costs)

### Access Logging
- **Target Bucket**: `s3://snowflake-media-analytics-meagan-066396400174-logs/`
- **Prefix**: `access-logs/`
- **Purpose**: Audit data access patterns

### Tags
```
Environment: production
Project: media-analytics
CostCenter: data-engineering
DataClassification: internal
Compliance: GDPR
Owner: data-platform-team
```

## Partitioning Strategy

### Time-Based Partitioning (Hive Style)
- **Pattern**: `year=YYYY/month=MM/day=DD/`
- **Benefits**: 
  - Query performance optimization
  - Cost-effective data pruning
  - Easy archival/deletion by date range

### Event-Type Partitioning (Optional)
- **Pattern**: `event_type={type}/year=YYYY/month=MM/`
- **Use Case**: Mixed event streams

## Data Quality & Validation

### File Validation Rules
1. **Schema Validation**: Parquet schema matches expected structure
2. **Size Validation**: Files between 100KB and 5GB
3. **Naming Convention**: Matches regex pattern
4. **Date Validation**: Partition date matches file content dates

### Error Handling
- **Failed Files Location**: `s3://.../errors/`
- **Rejected Records**: `s3://.../rejected/`
- **Retention**: 30 days for troubleshooting

## Setup Scripts

See companion files:
- `create_s3_bucket.sh` - AWS CLI commands to create bucket
- `setup_iam_roles.sh` - Create IAM roles and policies
- `configure_lifecycle.sh` - Apply lifecycle policies
- `generate_sample_data.py` - Generate sample media analytics data

## Cost Optimization

### Storage Classes
- **STANDARD**: Active data (<30 days)
- **STANDARD_IA**: Infrequent access (30-90 days)
- **GLACIER**: Archive (90-365 days)
- **DEEP_ARCHIVE**: Long-term retention (>365 days)

### Expected Monthly Costs (Estimate)
- Storage (100GB): ~$2.30/month (STANDARD)
- Requests: ~$0.50/month
- Data Transfer to Snowflake: $0 (same region)
- **Total**: ~$3/month (scales with data volume)

## Security Best Practices

1. ✅ Enable bucket versioning
2. ✅ Enable encryption at rest
3. ✅ Use IAM roles (not access keys)
4. ✅ Apply least privilege access
5. ✅ Enable access logging
6. ✅ Use VPC endpoints (private connectivity)
7. ✅ Implement bucket policies to deny unencrypted uploads
8. ✅ Enable MFA delete for production

## Next Steps

1. Create S3 bucket using provided scripts
2. Configure IAM roles for Snowflake integration
3. Upload sample data or connect data sources
4. Configure Snowflake external stages (see `../snowflake_stages/`)
5. Test COPY INTO commands
6. Set up monitoring and alerts

## References

- [Snowflake S3 Integration Guide](https://docs.snowflake.com/en/user-guide/data-load-s3)
- [AWS S3 Best Practices](https://docs.aws.amazon.com/AmazonS3/latest/userguide/security-best-practices.html)
- [Parquet File Format Specification](https://parquet.apache.org/docs/)
