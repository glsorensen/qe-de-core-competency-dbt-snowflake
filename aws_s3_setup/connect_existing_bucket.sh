#!/bin/bash
# Connect existing S3 bucket (qe-de-capstone-mahmed) to Snowflake
# AWS Region: us-west-2
# Owner: Meagan

set -e

BUCKET_NAME="qe-de-capstone-mahmed"
ACCOUNT_ID="066396400174"
REGION="us-west-2"
PROFILE="default"

echo "================================================"
echo "Connect S3 Bucket to Snowflake"
echo "================================================"
echo "Bucket: ${BUCKET_NAME}"
echo "Region: ${REGION}"
echo "Account: ${ACCOUNT_ID}"
echo ""

# Step 1: Get Snowflake VPC ID
echo "STEP 1: Get Snowflake VPC ID"
echo "================================================"
echo ""
echo "In Snowflake, run this command:"
echo ""
echo "  USE ROLE ACCOUNTADMIN;"
echo "  SELECT SYSTEM\$GET_SNOWFLAKE_PLATFORM_INFO();"
echo ""
echo "This will return JSON with your snowflakeVpcId"
echo ""
read -p "Have you retrieved your Snowflake VPC ID? (yes/no): " has_vpc

if [[ "$has_vpc" != "yes" ]]; then
    echo ""
    echo "Please run the SQL command above in Snowflake first."
    echo "Then re-run this script."
    exit 0
fi

echo ""
read -p "Enter your Snowflake VPC ID (e.g., vpc-abc123xyz): " SNOWFLAKE_VPC_ID
read -p "Confirm Snowflake region is us-west-2 (yes/no): " region_confirm

if [[ "$region_confirm" != "yes" ]]; then
    echo ""
    echo "⚠️  WARNING: Your S3 bucket is in us-west-2"
    echo "   Snowflake must also be in us-west-2 for VPC endpoint to work!"
    echo ""
    exit 1
fi

if [[ -z "$SNOWFLAKE_VPC_ID" ]]; then
    echo "Error: VPC ID is required"
    exit 1
fi

echo ""
echo "Configuration:"
echo "  S3 Bucket: ${BUCKET_NAME}"
echo "  AWS Region: ${REGION}"
echo "  Snowflake VPC: ${SNOWFLAKE_VPC_ID}"
echo ""

# Step 2: Create folder structure
echo "STEP 2: Creating folder structure in S3..."
echo "================================================"

folders=(
    "raw/viewership/"
    "raw/revenue/"
    "raw/content/"
    "raw/users/"
    "raw/subscriptions/"
    "stage/"
    "archive/"
    "errors/"
    "rejected/"
)

for folder in "${folders[@]}"; do
    echo "  Creating: ${folder}"
    aws s3api put-object \
        --bucket ${BUCKET_NAME} \
        --key "${folder}" \
        --profile ${PROFILE} 2>/dev/null || echo "    (folder may already exist)"
done

# Step 3: Create VPC endpoint bucket policy
echo ""
echo "STEP 3: Creating VPC endpoint bucket policy..."
echo "================================================"

cat > /tmp/bucket-policy.json <<EOF
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
            "Resource": "arn:aws:s3:::${BUCKET_NAME}/*",
            "Condition": {
                "StringEquals": {
                    "aws:SourceVpc": "${SNOWFLAKE_VPC_ID}"
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
            "Resource": "arn:aws:s3:::${BUCKET_NAME}",
            "Condition": {
                "StringEquals": {
                    "aws:SourceVpc": "${SNOWFLAKE_VPC_ID}"
                }
            }
        },
        {
            "Sid": "AllowCurrentAccountAccess",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::${ACCOUNT_ID}:root"
            },
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::${BUCKET_NAME}",
                "arn:aws:s3:::${BUCKET_NAME}/*"
            ]
        }
    ]
}
EOF

echo "Applying bucket policy..."
aws s3api put-bucket-policy \
    --bucket ${BUCKET_NAME} \
    --policy file:///tmp/bucket-policy.json \
    --profile ${PROFILE}

echo "✅ Bucket policy applied"

# Step 4: Generate Snowflake SQL
echo ""
echo "STEP 4: Generating Snowflake SQL scripts..."
echo "================================================"

cat > /tmp/snowflake_setup.sql <<'EOSQL'
-- ================================================
-- Snowflake Storage Integration Setup
-- Bucket: qe-de-capstone-mahmed (us-west-2)
-- Owner: Meagan
-- ================================================

USE ROLE ACCOUNTADMIN;

-- Create database and schemas
CREATE DATABASE IF NOT EXISTS MEDIA_ANALYTICS
  COMMENT = 'Media Analytics - Meagan Capstone';

USE DATABASE MEDIA_ANALYTICS;

CREATE SCHEMA IF NOT EXISTS RAW COMMENT = 'Bronze layer';
CREATE SCHEMA IF NOT EXISTS SILVER COMMENT = 'Silver layer';
CREATE SCHEMA IF NOT EXISTS GOLD COMMENT = 'Gold layer';

USE SCHEMA RAW;

-- ================================================
-- Create Storage Integration
-- ================================================
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
  COMMENT = 'Meagan capstone S3 integration';

-- ⚠️ CRITICAL: After running the above, run this:
DESC STORAGE INTEGRATION s3_capstone_integration;

-- Copy these 2 values:
-- 1. STORAGE_AWS_IAM_USER_ARN
-- 2. STORAGE_AWS_EXTERNAL_ID
-- You need them to create the IAM role in AWS!

-- ================================================
-- Create File Formats
-- ================================================
CREATE OR REPLACE FILE FORMAT parquet_format
  TYPE = 'PARQUET'
  COMPRESSION = 'SNAPPY';

CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = 'CSV'
  COMPRESSION = 'GZIP'
  FIELD_DELIMITER = '|'
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"';

CREATE OR REPLACE FILE FORMAT json_format
  TYPE = 'JSON'
  COMPRESSION = 'GZIP';

-- ================================================
-- Create External Stages
-- ================================================
CREATE OR REPLACE STAGE s3_viewership_stage
  STORAGE_INTEGRATION = s3_capstone_integration
  URL = 's3://qe-de-capstone-mahmed/raw/viewership/'
  FILE_FORMAT = parquet_format
  COMMENT = 'Viewership events';

CREATE OR REPLACE STAGE s3_revenue_stage
  STORAGE_INTEGRATION = s3_capstone_integration
  URL = 's3://qe-de-capstone-mahmed/raw/revenue/'
  FILE_FORMAT = parquet_format
  COMMENT = 'Revenue transactions';

CREATE OR REPLACE STAGE s3_content_stage
  STORAGE_INTEGRATION = s3_capstone_integration
  URL = 's3://qe-de-capstone-mahmed/raw/content/'
  FILE_FORMAT = parquet_format
  COMMENT = 'Content catalog';

CREATE OR REPLACE STAGE s3_users_stage
  STORAGE_INTEGRATION = s3_capstone_integration
  URL = 's3://qe-de-capstone-mahmed/raw/users/'
  FILE_FORMAT = parquet_format
  COMMENT = 'User profiles';

CREATE OR REPLACE STAGE s3_subscriptions_stage
  STORAGE_INTEGRATION = s3_capstone_integration
  URL = 's3://qe-de-capstone-mahmed/raw/subscriptions/'
  FILE_FORMAT = parquet_format
  COMMENT = 'Subscriptions';

-- ================================================
-- Test Connection (run after IAM role is created)
-- ================================================
LIST @s3_content_stage;
LIST @s3_viewership_stage;

-- If these work, you're connected! ✅

EOSQL

echo "✅ Snowflake SQL saved to: /tmp/snowflake_setup.sql"

# Step 5: Show next steps
cat > /tmp/next_steps.txt <<EOF
================================================
✅ S3 BUCKET CONFIGURED FOR SNOWFLAKE
================================================

S3 Bucket: ${BUCKET_NAME}
Region: ${REGION}
VPC Restriction: ${SNOWFLAKE_VPC_ID}

Folder Structure Created:
  ✅ raw/viewership/
  ✅ raw/revenue/
  ✅ raw/content/
  ✅ raw/users/
  ✅ raw/subscriptions/
  ✅ stage/
  ✅ archive/
  ✅ errors/

VPC Endpoint Policy: ✅ Applied

================================================
NEXT STEPS:
================================================

1️⃣  In Snowflake, run:
   /tmp/snowflake_setup.sql

2️⃣  After creating storage integration, run:
   DESC STORAGE INTEGRATION s3_capstone_integration;
   
   Copy these values:
   - STORAGE_AWS_IAM_USER_ARN
   - STORAGE_AWS_EXTERNAL_ID

3️⃣  Create IAM role in AWS:
   cd /Users/meagan.ahmed/Catalyst/qe-de-core-competency-dbt-snowflake/aws_s3_setup
   ./create_iam_role_for_snowflake.sh
   
   Enter the values from step 2 when prompted

4️⃣  Back in Snowflake, test the connection:
   LIST @s3_content_stage;
   
   If this works, you're all set! ✅

5️⃣  Upload sample data:
   python generate_sample_data.py --upload-to-s3

6️⃣  Load data into Snowflake:
   Use COPY INTO commands

================================================
FILES CREATED:
================================================
- /tmp/bucket-policy.json (applied to S3)
- /tmp/snowflake_setup.sql (run in Snowflake)
- /tmp/next_steps.txt (this file)

================================================
EOF

# Display next steps
cat /tmp/next_steps.txt

echo ""
echo "✅ Setup complete! Follow the steps above to connect Snowflake."
echo ""
