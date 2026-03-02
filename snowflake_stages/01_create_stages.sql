-- Snowflake External Stage Setup for AWS S3 Media Analytics Pipeline
-- AWS Account: 066396400174
-- Purpose: Configure storage integrations, file formats, and external stages for data ingestion

/*
========================================
PREREQUISITES
========================================
1. S3 bucket created: snowflake-media-analytics-066396400174
2. IAM role created with proper trust policy
3. Snowflake account with ACCOUNTADMIN privileges
4. AWS and Snowflake in same region (recommended)
*/

-- Use appropriate role
USE ROLE ACCOUNTADMIN;

-- Create or use database
CREATE DATABASE IF NOT EXISTS MEDIA_ANALYTICS;
USE DATABASE MEDIA_ANALYTICS;

-- Create schemas following Medallion Architecture
CREATE SCHEMA IF NOT EXISTS RAW;        -- Bronze: Raw data from S3
CREATE SCHEMA IF NOT EXISTS SILVER;     -- Silver: Cleaned/transformed
CREATE SCHEMA IF NOT EXISTS GOLD;       -- Gold: Business-ready analytics
CREATE SCHEMA IF NOT EXISTS LOGS;       -- Logging and observability

USE SCHEMA RAW;

/*
========================================
STEP 1: CREATE STORAGE INTEGRATION
========================================
Storage Integration allows Snowflake to securely access S3 without storing credentials.
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
  COMMENT = 'Integration for S3 media analytics data ingestion - Meagan capstone project';

/*
========================================
IMPORTANT: After creating storage integration, run:
========================================
DESC STORAGE INTEGRATION s3_media_analytics_integration;

Copy these values:
- STORAGE_AWS_IAM_USER_ARN
- STORAGE_AWS_EXTERNAL_ID

Then update the IAM role trust policy in AWS with these values!
*/

-- View storage integration details
DESC STORAGE INTEGRATION s3_media_analytics_integration;

-- Grant usage to appropriate roles
GRANT USAGE ON INTEGRATION s3_media_analytics_integration TO ROLE SYSADMIN;
GRANT USAGE ON INTEGRATION s3_media_analytics_integration TO ROLE DATA_ENGINEER;

/*
========================================
STEP 2: CREATE FILE FORMATS
========================================
Define file formats for different data types
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
STEP 3: CREATE EXTERNAL STAGES
========================================
External stages point to S3 locations for data ingestion
*/

-- Viewership data stage
CREATE OR REPLACE STAGE s3_viewership_stage
  STORAGE_INTEGRATION = s3_media_analytics_integration
  URL = 's3://snowflake-media-analytics-meagan-066396400174/raw/viewership/'
  FILE_FORMAT = parquet_format
  COMMENT = 'Stage for viewership event data';

-- Revenue data stage
CREATE OR REPLACE STAGE s3_revenue_stage
  STORAGE_INTEGRATION = s3_media_analytics_integration
  URL = 's3://snowflake-media-analytics-meagan-066396400174/raw/revenue/'
  FILE_FORMAT = parquet_format
  COMMENT = 'Stage for revenue transaction data';

-- Content catalog stage
CREATE OR REPLACE STAGE s3_content_stage
  STORAGE_INTEGRATION = s3_media_analytics_integration
  URL = 's3://snowflake-media-analytics-meagan-066396400174/raw/content/'
  FILE_FORMAT = parquet_format
  COMMENT = 'Stage for content metadata';

-- Users stage
CREATE OR REPLACE STAGE s3_users_stage
  STORAGE_INTEGRATION = s3_media_analytics_integration
  URL = 's3://snowflake-media-analytics-meagan-066396400174/raw/users/'
  FILE_FORMAT = parquet_format
  COMMENT = 'Stage for user profile data';

-- Subscriptions stage
CREATE OR REPLACE STAGE s3_subscriptions_stage
  STORAGE_INTEGRATION = s3_media_analytics_integration
  URL = 's3://snowflake-media-analytics-meagan-066396400174/raw/subscriptions/'
  FILE_FORMAT = parquet_format
  COMMENT = 'Stage for subscription data';

-- Generic staging area (for validation/testing)
CREATE OR REPLACE STAGE s3_stage_area
  STORAGE_INTEGRATION = s3_media_analytics_integration
  URL = 's3://snowflake-media-analytics-meagan-066396400174/stage/'
  FILE_FORMAT = parquet_format
  COMMENT = 'Generic staging area for data validation';

/*
========================================
STEP 4: GRANT PRIVILEGES
========================================
*/

-- Grant stage usage to roles
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

/*
========================================
STEP 5: VERIFY STAGE SETUP
========================================
Test that stages can access S3
*/

-- List files in each stage
LIST @s3_viewership_stage;
LIST @s3_revenue_stage;
LIST @s3_content_stage;
LIST @s3_users_stage;
LIST @s3_subscriptions_stage;

/*
========================================
STEP 6: CREATE TARGET TABLES (RAW LAYER)
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
) COMMENT = 'Raw viewership events from S3';

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
) COMMENT = 'Raw revenue transactions from S3';

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
) COMMENT = 'Raw content metadata from S3';

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
) COMMENT = 'Raw user profiles from S3';

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
) COMMENT = 'Raw subscription data from S3';

/*
========================================
SUCCESS!
========================================
Storage integration and stages are configured.

Next steps:
1. Run DESC STORAGE INTEGRATION to get IAM details
2. Update AWS IAM trust policy with Snowflake details
3. Upload sample data to S3 using generate_sample_data.py
4. Test data ingestion with COPY INTO commands (see copy_commands.sql)
5. Set up Snowpipe for continuous ingestion (see snowpipe_setup.sql)
*/

-- Show all created objects
SHOW INTEGRATIONS;
SHOW FILE FORMATS;
SHOW STAGES;
SHOW TABLES IN SCHEMA RAW;
