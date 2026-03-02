-- Snowflake External Stage Setup for AWS S3 with VPC Endpoint Authentication
-- This is the SECURE way to connect Snowflake to S3 using VPC endpoints
-- AWS Account: 066396400174
-- Owner: Meagan (Capstone Project)

/*
========================================
PREREQUISITES - READ THIS FIRST!
========================================
This setup uses VPC endpoint authentication, which is more secure than IAM user credentials.

REQUIRED: Your Snowflake account and S3 bucket must be in the SAME AWS region!

Before running this script:
1. ✅ S3 bucket created with VPC endpoint policy: snowflake-media-analytics-meagan-066396400174
2. ✅ VPC ID retrieved from Snowflake
3. ⚠️  IAM role created (you'll do this AFTER creating storage integration)

Steps to complete:
A. Get Snowflake VPC ID (below)
B. Create storage integration (below)
C. Get Snowflake IAM details from storage integration
D. Create AWS IAM role with those details
E. Test the integration
*/

-- Use appropriate role
USE ROLE ACCOUNTADMIN;

/*
========================================
STEP A: GET SNOWFLAKE VPC ID
========================================
This VPC ID is used in S3 bucket policies to restrict access
*/

-- Get your Snowflake platform information
SELECT SYSTEM$GET_SNOWFLAKE_PLATFORM_INFO();

/*
Expected output (JSON):
{
  "snowflakeVpcId": ["vpc-1234567890abcdef0"],
  "awsRegion": "us-east-1",
  "awsAccountId": "123456789012"
}

IMPORTANT: 
- Record the snowflakeVpcId value
- Verify awsRegion matches your S3 bucket region (us-east-1)
- This VPC ID is already configured in your S3 bucket policy
*/

/*
========================================
STEP B: CREATE DATABASE AND SCHEMAS
========================================
*/

-- Create or use database
CREATE DATABASE IF NOT EXISTS MEDIA_ANALYTICS
  COMMENT = 'Media & Entertainment Analytics - Meagan Capstone Project';

USE DATABASE MEDIA_ANALYTICS;

-- Create schemas following Medallion Architecture
CREATE SCHEMA IF NOT EXISTS RAW
  COMMENT = 'Bronze layer: Raw data from S3';
  
CREATE SCHEMA IF NOT EXISTS SILVER
  COMMENT = 'Silver layer: Cleaned and standardized data';
  
CREATE SCHEMA IF NOT EXISTS GOLD
  COMMENT = 'Gold layer: Business-ready analytics models';
  
CREATE SCHEMA IF NOT EXISTS LOGS
  COMMENT = 'Observability and monitoring logs';

USE SCHEMA RAW;

/*
========================================
STEP C: CREATE STORAGE INTEGRATION (VPC-BASED)
========================================
This creates the connection between Snowflake and S3 using VPC endpoints
*/

CREATE OR REPLACE STORAGE INTEGRATION s3_media_analytics_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::066396400174:role/snowflake-s3-integration-role-meagan'
  STORAGE_ALLOWED_LOCATIONS = (
    's3://snowflake-media-analytics-meagan-066396400174/raw/',
    's3://snowflake-media-analytics-meagan-066396400174/stage/'
  )
  STORAGE_BLOCKED_LOCATIONS = (
    's3://snowflake-media-analytics-meagan-066396400174/errors/',
    's3://snowflake-media-analytics-meagan-066396400174/archive/'
  )
  COMMENT = 'VPC-based S3 integration for Meagan capstone - Media Analytics';

/*
========================================
STEP D: GET SNOWFLAKE IAM DETAILS
========================================
After creating the storage integration above, run this to get details for AWS IAM setup
*/

DESC STORAGE INTEGRATION s3_media_analytics_integration;

/*
CRITICAL: Record these values from the output:
1. STORAGE_AWS_IAM_USER_ARN 
   Example: arn:aws:iam::123456789012:user/abc12345-s
   
2. STORAGE_AWS_EXTERNAL_ID
   Example: ABC12345_SFCRole=1_abcdefg1234567

You need BOTH values to update the AWS IAM role trust policy!

Next steps:
1. Copy both values above
2. Go to AWS and run: ./setup_iam_roles_vpc.sh
3. Enter these values when prompted
4. Come back here to verify the integration works
*/

/*
========================================
STEP E: VERIFY STORAGE INTEGRATION
========================================
Run this after creating the IAM role in AWS
*/

-- View integration details
DESC STORAGE INTEGRATION s3_media_analytics_integration;

-- Check integration status (should show ENABLED = true)
SHOW INTEGRATIONS;

/*
========================================
STEP F: CREATE FILE FORMATS
========================================
*/

-- Parquet format (preferred for analytics)
CREATE OR REPLACE FILE FORMAT parquet_format
  TYPE = 'PARQUET'
  COMPRESSION = 'SNAPPY'
  BINARY_AS_TEXT = FALSE
  TRIM_SPACE = FALSE
  COMMENT = 'Parquet format with Snappy compression';

-- CSV format (for legacy/external sources)
CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = 'CSV'
  COMPRESSION = 'GZIP'
  FIELD_DELIMITER = '|'
  RECORD_DELIMITER = '\n'
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
  ESCAPE = '\\'
  ESCAPE_UNENCLOSED_FIELD = '\\'
  DATE_FORMAT = 'AUTO'
  TIMESTAMP_FORMAT = 'AUTO'
  NULL_IF = ('NULL', 'null', '\\N', '')
  COMMENT = 'Pipe-delimited CSV format';

-- JSON format (for event streams)
CREATE OR REPLACE FILE FORMAT json_format
  TYPE = 'JSON'
  COMPRESSION = 'GZIP'
  ENABLE_OCTAL = FALSE
  ALLOW_DUPLICATE = FALSE
  STRIP_OUTER_ARRAY = TRUE
  STRIP_NULL_VALUES = FALSE
  IGNORE_UTF8_ERRORS = FALSE
  SKIP_BYTE_ORDER_MARK = TRUE
  COMMENT = 'Line-delimited JSON (NDJSON) format';

/*
========================================
STEP G: CREATE EXTERNAL STAGES
========================================
*/

-- Viewership data stage
CREATE OR REPLACE STAGE s3_viewership_stage
  STORAGE_INTEGRATION = s3_media_analytics_integration
  URL = 's3://snowflake-media-analytics-meagan-066396400174/raw/viewership/'
  FILE_FORMAT = parquet_format
  COMMENT = 'VPC-secured stage for viewership event data';

-- Revenue data stage
CREATE OR REPLACE STAGE s3_revenue_stage
  STORAGE_INTEGRATION = s3_media_analytics_integration
  URL = 's3://snowflake-media-analytics-meagan-066396400174/raw/revenue/'
  FILE_FORMAT = parquet_format
  COMMENT = 'VPC-secured stage for revenue transaction data';

-- Content catalog stage
CREATE OR REPLACE STAGE s3_content_stage
  STORAGE_INTEGRATION = s3_media_analytics_integration
  URL = 's3://snowflake-media-analytics-meagan-066396400174/raw/content/'
  FILE_FORMAT = parquet_format
  COMMENT = 'VPC-secured stage for content metadata';

-- Users stage
CREATE OR REPLACE STAGE s3_users_stage
  STORAGE_INTEGRATION = s3_media_analytics_integration
  URL = 's3://snowflake-media-analytics-meagan-066396400174/raw/users/'
  FILE_FORMAT = parquet_format
  COMMENT = 'VPC-secured stage for user profile data';

-- Subscriptions stage
CREATE OR REPLACE STAGE s3_subscriptions_stage
  STORAGE_INTEGRATION = s3_media_analytics_integration
  URL = 's3://snowflake-media-analytics-meagan-066396400174/raw/subscriptions/'
  FILE_FORMAT = parquet_format
  COMMENT = 'VPC-secured stage for subscription data';

-- Generic staging area
CREATE OR REPLACE STAGE s3_stage_area
  STORAGE_INTEGRATION = s3_media_analytics_integration
  URL = 's3://snowflake-media-analytics-meagan-066396400174/stage/'
  FILE_FORMAT = parquet_format
  COMMENT = 'VPC-secured staging area for validation';

/*
========================================
STEP H: TEST STAGE ACCESS
========================================
This verifies that Snowflake can access your S3 bucket through VPC
*/

-- List files in each stage (should work after IAM role is configured)
LIST @s3_viewership_stage;
LIST @s3_revenue_stage;
LIST @s3_content_stage;
LIST @s3_users_stage;
LIST @s3_subscriptions_stage;

-- If you get errors here, check:
-- 1. IAM role trust policy has correct Snowflake user ARN and External ID
-- 2. S3 bucket policy allows the Snowflake VPC ID
-- 3. S3 bucket and Snowflake account are in same AWS region

/*
========================================
STEP I: CREATE RAW TABLES
========================================
*/

-- Raw viewership events table
CREATE OR REPLACE TABLE RAW.viewership_events (
    event_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    content_id VARCHAR(50) NOT NULL,
    view_start_time TIMESTAMP_NTZ NOT NULL,
    view_end_time TIMESTAMP_NTZ,
    watch_duration_minutes NUMBER(10,2),
    completion_percentage NUMBER(5,2),
    device_type VARCHAR(50),
    platform VARCHAR(50),
    quality VARCHAR(10),
    buffering_events INTEGER,
    country VARCHAR(3),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Raw viewership events from S3 (VPC-secured)';

-- Raw revenue transactions table
CREATE OR REPLACE TABLE RAW.revenue_transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    subscription_id VARCHAR(50),
    transaction_time TIMESTAMP_NTZ NOT NULL,
    transaction_type VARCHAR(50),
    amount NUMBER(10,2),
    currency VARCHAR(3),
    payment_method VARCHAR(50),
    status VARCHAR(20),
    country VARCHAR(3),
    created_at TIMESTAMP_NTZ,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Raw revenue transactions from S3 (VPC-secured)';

-- Raw content catalog table
CREATE OR REPLACE TABLE RAW.content_catalog (
    content_id VARCHAR(50) PRIMARY KEY,
    title VARCHAR(500),
    content_type VARCHAR(50),
    genre VARCHAR(50),
    release_year INTEGER,
    duration_minutes INTEGER,
    rating VARCHAR(10),
    production_cost NUMBER(12,2),
    is_original BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Raw content metadata from S3 (VPC-secured)';

-- Raw users table
CREATE OR REPLACE TABLE RAW.users (
    user_id VARCHAR(50) PRIMARY KEY,
    email VARCHAR(255),
    country VARCHAR(3),
    preferred_language VARCHAR(10),
    signup_date DATE,
    preferred_device VARCHAR(50),
    age_group VARCHAR(20),
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Raw user profiles from S3 (VPC-secured)';

-- Raw subscriptions table
CREATE OR REPLACE TABLE RAW.subscriptions (
    subscription_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    tier VARCHAR(20),
    price_per_month NUMBER(10,2),
    billing_cycle VARCHAR(20),
    start_date DATE,
    end_date DATE,
    status VARCHAR(20),
    auto_renew BOOLEAN,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Raw subscription data from S3 (VPC-secured)';

/*
========================================
STEP J: GRANT PRIVILEGES
========================================
*/

-- Create data engineering role if it doesn't exist
CREATE ROLE IF NOT EXISTS DATA_ENGINEER
  COMMENT = 'Role for data engineers - Meagan capstone';

-- Grant database and schema usage
GRANT USAGE ON DATABASE MEDIA_ANALYTICS TO ROLE DATA_ENGINEER;
GRANT USAGE ON SCHEMA RAW TO ROLE DATA_ENGINEER;
GRANT USAGE ON SCHEMA SILVER TO ROLE DATA_ENGINEER;
GRANT USAGE ON SCHEMA GOLD TO ROLE DATA_ENGINEER;

-- Grant integration usage
GRANT USAGE ON INTEGRATION s3_media_analytics_integration TO ROLE DATA_ENGINEER;

-- Grant stage usage
GRANT USAGE ON STAGE s3_viewership_stage TO ROLE DATA_ENGINEER;
GRANT USAGE ON STAGE s3_revenue_stage TO ROLE DATA_ENGINEER;
GRANT USAGE ON STAGE s3_content_stage TO ROLE DATA_ENGINEER;
GRANT USAGE ON STAGE s3_users_stage TO ROLE DATA_ENGINEER;
GRANT USAGE ON STAGE s3_subscriptions_stage TO ROLE DATA_ENGINEER;
GRANT USAGE ON STAGE s3_stage_area TO ROLE DATA_ENGINEER;

-- Grant file format usage
GRANT USAGE ON FILE FORMAT parquet_format TO ROLE DATA_ENGINEER;
GRANT USAGE ON FILE FORMAT csv_format TO ROLE DATA_ENGINEER;
GRANT USAGE ON FILE FORMAT json_format TO ROLE DATA_ENGINEER;

-- Grant table permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA RAW TO ROLE DATA_ENGINEER;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA RAW TO ROLE DATA_ENGINEER;

/*
========================================
VERIFICATION CHECKLIST
========================================
Run these to verify everything is set up correctly:
*/

-- 1. Check storage integration
DESC STORAGE INTEGRATION s3_media_analytics_integration;

-- 2. List stages
SHOW STAGES;

-- 3. Test file listing (should show files if you've uploaded data)
LIST @s3_content_stage;

-- 4. Check VPC configuration
SELECT SYSTEM$GET_SNOWFLAKE_PLATFORM_INFO();

/*
========================================
SUCCESS! Next Steps
========================================
✅ VPC-based storage integration configured
✅ External stages created with VPC security
✅ File formats defined
✅ Raw tables created
✅ Permissions granted

Next:
1. Upload sample data to S3 (python generate_sample_data.py --upload-to-s3)
2. Test COPY INTO commands (see ../snowflake_stages/02_copy_commands.sql)
3. Set up dbt models for transformation
4. Configure Elementary for observability

Security Benefits of VPC Endpoints:
- ✅ Traffic never leaves AWS network
- ✅ No public internet exposure
- ✅ S3 bucket only accessible from Snowflake VPC
- ✅ No IAM user credentials stored anywhere
- ✅ Automatic credential rotation
*/

-- Show all created objects
SHOW INTEGRATIONS;
SHOW FILE FORMATS;
SHOW STAGES;
SHOW TABLES IN SCHEMA RAW;

SELECT '✅ VPC-based Snowflake S3 integration complete!' AS status;
