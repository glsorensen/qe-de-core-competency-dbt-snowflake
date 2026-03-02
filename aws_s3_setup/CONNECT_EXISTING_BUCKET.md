# Connect Your S3 Bucket to Snowflake - Step by Step Guide
## Bucket: qe-de-capstone-mahmed (us-west-2)

Great! You've created your S3 bucket. Now let's connect it to Snowflake using the secure VPC endpoint method.

---

## 📋 Current Setup

- ✅ **S3 Bucket**: `qe-de-capstone-mahmed`
- ✅ **AWS Region**: `us-west-2` (Oregon)
- ✅ **AWS Account**: `066396400174`

---

## 🎯 Connection Steps

### Step 1: Get Your Snowflake VPC ID

**In Snowflake (Web UI or SnowSQL):**

```sql
-- Log in to Snowflake and run these commands
USE ROLE ACCOUNTADMIN;

SELECT SYSTEM$GET_SNOWFLAKE_PLATFORM_INFO();
```

**This will return JSON like:**
```json
{
  "snowflakeVpcId": ["vpc-abc123xyz"],
  "awsRegion": "us-west-2",
  "awsAccountId": "123456789012"
}
```

**⚠️ IMPORTANT CHECK:**
- Your Snowflake `awsRegion` **MUST** be `us-west-2` (same as your S3 bucket)
- If it's different, the VPC endpoint approach won't work

**✍️ Write down your Snowflake VPC ID here:** `vpc-___________________`

---

### Step 2: Create Folder Structure in S3

Let's organize your bucket with proper folders:

```bash
# Create folder structure
aws s3api put-object --bucket qe-de-capstone-mahmed --key raw/viewership/
aws s3api put-object --bucket qe-de-capstone-mahmed --key raw/revenue/
aws s3api put-object --bucket qe-de-capstone-mahmed --key raw/content/
aws s3api put-object --bucket qe-de-capstone-mahmed --key raw/users/
aws s3api put-object --bucket qe-de-capstone-mahmed --key raw/subscriptions/
aws s3api put-object --bucket qe-de-capstone-mahmed --key stage/
aws s3api put-object --bucket qe-de-capstone-mahmed --key archive/
aws s3api put-object --bucket qe-de-capstone-mahmed --key errors/
```

Or run this script:

```bash
cd /Users/meagan.ahmed/Catalyst/qe-de-core-competency-dbt-snowflake/aws_s3_setup
./setup_existing_bucket.sh
```

---

### Step 3: Add VPC Endpoint Policy to S3 Bucket

**After you have your Snowflake VPC ID from Step 1:**

Create a bucket policy file (`/tmp/bucket-policy.json`):

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowSnowflakeVPCAccess",
            "Effect": "Allow",
            "Principal": "*",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": "arn:aws:s3:::qe-de-capstone-mahmed/*",
            "Condition": {
                "StringEquals": {
                    "aws:SourceVpc": "vpc-YOUR_SNOWFLAKE_VPC_ID_HERE"
                }
            }
        },
        {
            "Sid": "AllowSnowflakeVPCList",
            "Effect": "Allow",
            "Principal": "*",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": "arn:aws:s3:::qe-de-capstone-mahmed",
            "Condition": {
                "StringEquals": {
                    "aws:SourceVpc": "vpc-YOUR_SNOWFLAKE_VPC_ID_HERE"
                }
            }
        },
        {
            "Sid": "AllowCurrentAccountAccess",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::066396400174:root"
            },
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::qe-de-capstone-mahmed",
                "arn:aws:s3:::qe-de-capstone-mahmed/*"
            ]
        }
    ]
}
```

**Replace `vpc-YOUR_SNOWFLAKE_VPC_ID_HERE` with your actual VPC ID from Step 1!**

Then apply the policy:

```bash
aws s3api put-bucket-policy \
    --bucket qe-de-capstone-mahmed \
    --policy file:///tmp/bucket-policy.json
```

---

### Step 4: Create Snowflake Storage Integration

**In Snowflake:**

```sql
USE ROLE ACCOUNTADMIN;
USE DATABASE MEDIA_ANALYTICS;  -- Or create if needed: CREATE DATABASE MEDIA_ANALYTICS;

CREATE OR REPLACE STORAGE INTEGRATION s3_capstone_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::066396400174:role/snowflake-capstone-role'
  STORAGE_ALLOWED_LOCATIONS = (
    's3://qe-de-capstone-mahmed/raw/',
    's3://qe-de-capstone-mahmed/stage/'
  )
  STORAGE_BLOCKED_LOCATIONS = (
    's3://qe-de-capstone-mahmed/errors/',
    's3://qe-de-capstone-mahmed/archive/'
  )
  COMMENT = 'Meagan capstone - S3 integration for media analytics';
```

---

### Step 5: Get Snowflake IAM Details

**In Snowflake (immediately after creating storage integration):**

```sql
DESC STORAGE INTEGRATION s3_capstone_integration;
```

**Look for these 2 values in the output:**

| Property Name | What to Copy |
|--------------|--------------|
| `STORAGE_AWS_IAM_USER_ARN` | Full ARN like: `arn:aws:iam::123456789012:user/abc12345-s` |
| `STORAGE_AWS_EXTERNAL_ID` | ID like: `ABC12345_SFCRole=1_xyz` |

**✍️ Write these down - you need them for the next step!**

---

### Step 6: Create IAM Role in AWS

**In AWS (using AWS CLI or Console):**

**Trust Policy** (`/tmp/trust-policy.json`):
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "PASTE_STORAGE_AWS_IAM_USER_ARN_HERE"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "PASTE_STORAGE_AWS_EXTERNAL_ID_HERE"
        }
      }
    }
  ]
}
```

**Create the role:**
```bash
aws iam create-role \
    --role-name snowflake-capstone-role \
    --assume-role-policy-document file:///tmp/trust-policy.json \
    --description "Snowflake access to S3 for Meagan capstone"
```

**Access Policy** (`/tmp/access-policy.json`):
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::qe-de-capstone-mahmed",
        "arn:aws:s3:::qe-de-capstone-mahmed/*"
      ]
    }
  ]
}
```

**Create and attach policy:**
```bash
# Create policy
aws iam create-policy \
    --policy-name snowflake-capstone-policy \
    --policy-document file:///tmp/access-policy.json

# Attach to role
aws iam attach-role-policy \
    --role-name snowflake-capstone-role \
    --policy-arn arn:aws:iam::066396400174:policy/snowflake-capstone-policy
```

---

### Step 7: Create External Stages in Snowflake

**In Snowflake:**

```sql
USE ROLE ACCOUNTADMIN;
USE DATABASE MEDIA_ANALYTICS;
CREATE SCHEMA IF NOT EXISTS RAW;
USE SCHEMA RAW;

-- Create file format
CREATE OR REPLACE FILE FORMAT parquet_format
  TYPE = 'PARQUET'
  COMPRESSION = 'SNAPPY';

-- Create external stages
CREATE OR REPLACE STAGE s3_viewership_stage
  STORAGE_INTEGRATION = s3_capstone_integration
  URL = 's3://qe-de-capstone-mahmed/raw/viewership/'
  FILE_FORMAT = parquet_format;

CREATE OR REPLACE STAGE s3_revenue_stage
  STORAGE_INTEGRATION = s3_capstone_integration
  URL = 's3://qe-de-capstone-mahmed/raw/revenue/'
  FILE_FORMAT = parquet_format;

CREATE OR REPLACE STAGE s3_content_stage
  STORAGE_INTEGRATION = s3_capstone_integration
  URL = 's3://qe-de-capstone-mahmed/raw/content/'
  FILE_FORMAT = parquet_format;

CREATE OR REPLACE STAGE s3_users_stage
  STORAGE_INTEGRATION = s3_capstone_integration
  URL = 's3://qe-de-capstone-mahmed/raw/users/'
  FILE_FORMAT = parquet_format;

CREATE OR REPLACE STAGE s3_subscriptions_stage
  STORAGE_INTEGRATION = s3_capstone_integration
  URL = 's3://qe-de-capstone-mahmed/raw/subscriptions/'
  FILE_FORMAT = parquet_format;
```

---

### Step 8: Test the Connection

**In Snowflake:**

```sql
-- List files in each stage (will be empty until you upload data)
LIST @s3_viewership_stage;
LIST @s3_content_stage;

-- If these work without errors, you're connected! ✅
```

**If you get errors:**
- "Access Denied" → Check IAM role trust policy has correct ARN and External ID
- "Invalid stage" → Check storage integration is enabled
- "Region mismatch" → Snowflake and S3 must be in same region

---

## 🚀 Quick Setup Script

I've created an automated script to help you:

```bash
cd /Users/meagan.ahmed/Catalyst/qe-de-core-competency-dbt-snowflake/aws_s3_setup
./connect_existing_bucket.sh
```

This script will:
1. Prompt for your Snowflake VPC ID
2. Create S3 bucket policy
3. Set up folder structure
4. Generate SQL scripts for Snowflake
5. Create IAM role template

---

## 📊 What You'll Have After Setup

```
S3: qe-de-capstone-mahmed (us-west-2)
  ├─ raw/
  │  ├─ viewership/
  │  ├─ revenue/
  │  ├─ content/
  │  ├─ users/
  │  └─ subscriptions/
  ├─ stage/
  ├─ archive/
  └─ errors/
          ↕️ VPC Endpoint (Secure)
Snowflake Storage Integration
  └─ External Stages
     └─ COPY INTO / Snowpipe
```

---

## ✅ Verification Checklist

- [ ] Got Snowflake VPC ID from `SYSTEM$GET_SNOWFLAKE_PLATFORM_INFO()`
- [ ] Verified Snowflake region = us-west-2
- [ ] Created S3 folder structure
- [ ] Applied VPC endpoint bucket policy
- [ ] Created storage integration in Snowflake
- [ ] Got IAM User ARN and External ID from DESC STORAGE INTEGRATION
- [ ] Created IAM role with trust policy
- [ ] Created and attached IAM policy
- [ ] Created external stages in Snowflake
- [ ] Successfully ran LIST @stage_name

---

## 🆘 Need Help?

Run the automated setup:
```bash
cd /Users/meagan.ahmed/Catalyst/qe-de-core-competency-dbt-snowflake/aws_s3_setup
./connect_existing_bucket.sh
```

Or ask me for help with any step!

---

## 📚 Next Steps

Once connected:
1. Upload sample data to S3
2. Load data with COPY INTO commands
3. Build dbt transformation models
4. Set up Elementary for observability
