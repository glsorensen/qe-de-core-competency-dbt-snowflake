# VPC-Based Snowflake S3 Integration Setup Guide
## Meagan's Capstone Project - Secure Configuration

This guide walks you through setting up **VPC endpoint authentication** between Snowflake and S3. This is more secure than IAM user credentials because:
- Traffic never leaves the AWS network
- No credentials to manage or rotate
- S3 bucket is only accessible from your Snowflake VPC
- Meets enterprise security requirements

---

## 🔐 Prerequisites

- ✅ AWS Account: 066396400174
- ✅ AWS CLI configured with credentials
- ✅ Snowflake account with ACCOUNTADMIN access
- ⚠️ **CRITICAL**: Snowflake and S3 must be in the **SAME AWS region**

---

## 📋 Setup Steps (In Order!)

### Step 1: Get Snowflake VPC ID

**In Snowflake (Web UI or SnowSQL):**

```sql
USE ROLE ACCOUNTADMIN;

SELECT SYSTEM$GET_SNOWFLAKE_PLATFORM_INFO();
```

**Expected Output:**
```json
{
  "snowflakeVpcId": ["vpc-1234567890abcdef0"],
  "awsRegion": "us-east-1",
  "awsAccountId": "123456789012"
}
```

**Record:**
- ✍️ snowflakeVpcId: `vpc-___________________`
- ✍️ awsRegion: `___________` (must match your S3 bucket region!)

---

### Step 2: Save VPC Configuration

**On your local machine:**

```bash
cd /Users/meagan.ahmed/Catalyst/qe-de-core-competency-dbt-snowflake/aws_s3_setup

# Make scripts executable
chmod +x get_snowflake_vpc_ids.sh create_s3_bucket_vpc.sh setup_iam_roles_vpc.sh

# Run VPC configuration script
./get_snowflake_vpc_ids.sh
```

**The script will:**
1. Prompt you for your Snowflake VPC ID
2. Save configuration to `/tmp/snowflake_vpc_config.env`
3. Display next steps

**Enter your VPC ID when prompted:**
```
Enter your Snowflake VPC ID: vpc-1234567890abcdef0
```

---

### Step 3: Create S3 Buckets with VPC Policy

```bash
# This creates buckets that ONLY allow access from Snowflake VPC
./create_s3_bucket_vpc.sh
```

**What this creates:**
- ✅ Main bucket: `snowflake-media-analytics-meagan-066396400174`
- ✅ Logs bucket: `snowflake-media-analytics-meagan-066396400174-logs`
- ✅ VPC endpoint policy (restricts access to Snowflake VPC only)
- ✅ Encryption, versioning, logging enabled
- ✅ Folder structure (raw/, stage/, archive/, errors/)

**Expected output:**
```
✅ S3 Infrastructure Created Successfully!
Main Bucket: s3://snowflake-media-analytics-meagan-066396400174
VPC Restriction: vpc-1234567890abcdef0
```

---

### Step 4: Create Snowflake Storage Integration

**In Snowflake:**

```sql
-- Copy from: snowflake_stages/01_create_stages_vpc.sql
-- Or run the entire script

USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION s3_media_analytics_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::066396400174:role/snowflake-s3-integration-role-meagan'
  STORAGE_ALLOWED_LOCATIONS = (
    's3://snowflake-media-analytics-meagan-066396400174/raw/',
    's3://snowflake-media-analytics-meagan-066396400174/stage/'
  )
  COMMENT = 'VPC-based S3 integration - Meagan capstone';
```

---

### Step 5: Get Snowflake IAM Details

**In Snowflake (immediately after creating storage integration):**

```sql
DESC STORAGE INTEGRATION s3_media_analytics_integration;
```

**Record these 2 critical values:**

| Property | Example Value | Your Value |
|----------|---------------|------------|
| STORAGE_AWS_IAM_USER_ARN | arn:aws:iam::123456789012:user/abc12345-s | ✍️ _________________ |
| STORAGE_AWS_EXTERNAL_ID | ABC12345_SFCRole=1_xyz | ✍️ _________________ |

**Save these values!** You need them for the next step.

---

### Step 6: Create AWS IAM Role with Snowflake Trust

**On your local machine:**

```bash
./setup_iam_roles_vpc.sh
```

**The script will prompt you for:**
1. Snowflake AWS Account ID (from STORAGE_AWS_IAM_USER_ARN)
2. Snowflake External ID (from STORAGE_AWS_EXTERNAL_ID)

**Example:**
```
Enter Snowflake AWS Account ID: 123456789012
Enter Snowflake External ID: ABC12345_SFCRole=1_xyz
```

**What this creates:**
- ✅ IAM Role: `snowflake-s3-integration-role-meagan`
- ✅ IAM Policy with VPC restrictions
- ✅ Trust policy allowing Snowflake to assume the role
- ✅ Configuration file saved to `/tmp/snowflake_integration_config.txt`

---

### Step 7: Verify Integration Works

**In Snowflake:**

```sql
-- Test listing files in S3 through the external stage
LIST @s3_content_stage;

-- If this works, you're all set! ✅
-- If you get an error, see Troubleshooting section below
```

---

### Step 8: Configure Lifecycle Policies (Optional but Recommended)

**On your local machine:**

```bash
cd /Users/meagan.ahmed/Catalyst/qe-de-core-competency-dbt-snowflake/aws_s3_setup
./configure_lifecycle.sh
```

**This sets up automatic cost optimization:**
- raw/ files: Transition to cheaper storage classes over time
- stage/ files: Auto-delete after 7 days
- errors/ files: Auto-delete after 30 days

---

## ✅ Verification Checklist

After completing all steps, verify:

```sql
-- In Snowflake:

-- 1. Check storage integration
DESC STORAGE INTEGRATION s3_media_analytics_integration;
-- Should show: ENABLED = true

-- 2. List external stages
SHOW STAGES;
-- Should show 6 stages (viewership, revenue, content, users, subscriptions, stage_area)

-- 3. Test S3 access
LIST @s3_viewership_stage;
-- Should list files (or empty if no files uploaded yet)

-- 4. Verify VPC configuration
SELECT SYSTEM$GET_SNOWFLAKE_PLATFORM_INFO();
-- Confirm VPC ID matches what you configured
```

---

## 🆘 Troubleshooting

### Error: "Access Denied" when listing stage

**Causes:**
1. IAM role trust policy doesn't have correct Snowflake user ARN
2. S3 bucket policy doesn't allow Snowflake VPC
3. IAM policy not attached to role

**Fix:**
```bash
# Check IAM role exists
aws iam get-role --role-name snowflake-s3-integration-role-meagan

# Check trust policy
aws iam get-role --role-name snowflake-s3-integration-role-meagan --query 'Role.AssumeRolePolicyDocument'

# Check attached policies
aws iam list-attached-role-policies --role-name snowflake-s3-integration-role-meagan

# Re-run IAM setup if needed
./setup_iam_roles_vpc.sh
```

### Error: "Region mismatch"

**Your Snowflake account and S3 bucket MUST be in the same region.**

Check Snowflake region:
```sql
SELECT SYSTEM$GET_SNOWFLAKE_PLATFORM_INFO();
```

Check S3 bucket region:
```bash
aws s3api get-bucket-location --bucket snowflake-media-analytics-meagan-066396400174
```

If they don't match, you need to either:
- Create S3 bucket in Snowflake's region, OR
- Use a different Snowflake account in the same region as S3

### Error: "VPC endpoint not found"

This means your S3 bucket policy has an incorrect VPC ID.

**Fix:**
1. Get correct VPC ID from Snowflake: `SELECT SYSTEM$GET_SNOWFLAKE_PLATFORM_INFO();`
2. Update S3 bucket policy with correct VPC ID
3. Re-run: `./create_s3_bucket_vpc.sh`

---

## 🎯 Next Steps

Once verification is complete:

1. **Generate Sample Data:**
   ```bash
   pip install pandas numpy pyarrow boto3
   python generate_sample_data.py --upload-to-s3
   ```

2. **Test Data Loading:**
   ```sql
   -- In Snowflake, run: snowflake_stages/02_copy_commands.sql
   COPY INTO RAW.content_catalog FROM @s3_content_stage
   FILE_FORMAT = (FORMAT_NAME = parquet_format)
   PATTERN = '.*content_catalog.*\.parquet';
   ```

3. **Set Up dbt Models:**
   - Create staging models (Silver layer)
   - Create dimensional models (Gold layer)
   - Run dbt transformations

4. **Configure Elementary for Observability**

---

## 📊 Architecture Summary

```
┌─────────────────────────────────────────────────────┐
│                AWS Account: 066396400174            │
├─────────────────────────────────────────────────────┤
│                                                     │
│  ┌─────────────────────────────────────────────┐   │
│  │  S3 Bucket (VPC Endpoint Policy)           │   │
│  │  snowflake-media-analytics-meagan-*        │   │
│  │                                             │   │
│  │  ├─ raw/viewership/                        │   │
│  │  ├─ raw/revenue/                           │   │
│  │  ├─ raw/content/                           │   │
│  │  └─ ...                                    │   │
│  │                                             │   │
│  │  Policy: ONLY allow VPC vpc-xxxxx          │   │
│  └─────────────────────────────────────────────┘   │
│                      ▲                              │
│                      │ VPC Endpoint                 │
│                      │ (Private Network)            │
│  ┌───────────────────▼──────────────────────────┐  │
│  │  IAM Role: snowflake-s3-integration-*       │  │
│  │  Trust Policy: Allow Snowflake Account     │  │
│  │  Permissions: S3 Read/Write                │  │
│  └─────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
                      ▲
                      │ AssumeRole
                      │ (Temporary Credentials)
┌─────────────────────▼──────────────────────────────┐
│            Snowflake Account                        │
│            VPC: vpc-xxxxx                          │
│                                                     │
│  ┌─────────────────────────────────────────────┐   │
│  │  Storage Integration                       │   │
│  │  s3_media_analytics_integration            │   │
│  └─────────────────────────────────────────────┘   │
│                      │                              │
│  ┌─────────────────────────────────────────────┐   │
│  │  External Stages                           │   │
│  │  - s3_viewership_stage                     │   │
│  │  - s3_revenue_stage                        │   │
│  │  - ...                                     │   │
│  └─────────────────────────────────────────────┘   │
│                      │                              │
│  ┌─────────────────────────────────────────────┐   │
│  │  COPY INTO / Snowpipe                      │   │
│  │  Load data from S3 → Snowflake tables     │   │
│  └─────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

---

## 🔒 Security Benefits

✅ **Network Isolation**: Traffic stays on AWS private network  
✅ **No Public Internet**: S3 never exposed publicly  
✅ **VPC Restriction**: Only Snowflake VPC can access S3  
✅ **No Stored Credentials**: Temporary credentials via AssumeRole  
✅ **Automatic Rotation**: AWS handles credential rotation  
✅ **Audit Trail**: CloudTrail logs all access attempts  

This setup meets enterprise security standards and is production-ready! 🎉

---

## 📚 Resources

- [Snowflake Storage Integration Docs](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration)
- [AWS VPC Endpoints](https://docs.aws.amazon.com/vpc/latest/privatelink/vpc-endpoints-s3.html)
- [S3 Bucket Policies](https://docs.aws.amazon.com/AmazonS3/latest/userguide/example-bucket-policies-vpc-endpoint.html)
