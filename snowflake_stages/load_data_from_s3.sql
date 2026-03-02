-- ================================================
-- Load Data from S3 to Snowflake RAW Tables
-- Run this AFTER you get USAGE privileges on the integration
-- ================================================

USE ROLE "SNOWFLAKE-ILABS-QECATALYST";
USE DATABASE MEDIA_ANALYTICS;
USE SCHEMA RAW;

-- Create stages (run after getting USAGE privileges)
CREATE OR REPLACE STAGE s3_viewership_stage
  URL = 's3://qe-de-capstone-mahmed/raw/viewership/'
  STORAGE_INTEGRATION = s3_capstone_integration;

CREATE OR REPLACE STAGE s3_revenue_stage
  URL = 's3://qe-de-capstone-mahmed/raw/revenue/'
  STORAGE_INTEGRATION = s3_capstone_integration;

CREATE OR REPLACE STAGE s3_content_stage
  URL = 's3://qe-de-capstone-mahmed/raw/content/'
  STORAGE_INTEGRATION = s3_capstone_integration;

CREATE OR REPLACE STAGE s3_users_stage
  URL = 's3://qe-de-capstone-mahmed/raw/users/'
  STORAGE_INTEGRATION = s3_capstone_integration;

-- Verify you can see the files
LIST @s3_viewership_stage;
LIST @s3_revenue_stage;
LIST @s3_content_stage;
LIST @s3_users_stage;

-- Create file formats
CREATE OR REPLACE FILE FORMAT parquet_format
  TYPE = 'PARQUET';

CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null', '')
  EMPTY_FIELD_AS_NULL = TRUE;

CREATE OR REPLACE FILE FORMAT json_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = FALSE;

-- Create raw tables
CREATE OR REPLACE TABLE raw_viewership (
    event_id VARCHAR,
    user_id VARCHAR,
    content_id VARCHAR,
    event_timestamp TIMESTAMP,
    watch_duration_seconds INTEGER,
    device_type VARCHAR,
    quality VARCHAR,
    buffer_count INTEGER
);

CREATE OR REPLACE TABLE raw_revenue (
    transaction_id VARCHAR,
    user_id VARCHAR,
    transaction_timestamp TIMESTAMP,
    transaction_type VARCHAR,
    amount_usd DECIMAL(10,2),
    payment_method VARCHAR,
    currency VARCHAR
);

CREATE OR REPLACE TABLE raw_content (
    content_id VARCHAR,
    title VARCHAR,
    genre VARCHAR,
    duration_minutes INTEGER,
    release_year INTEGER,
    rating VARCHAR,
    content_type VARCHAR
);

CREATE OR REPLACE TABLE raw_users (
    user_id VARCHAR,
    signup_date DATE,
    country VARCHAR,
    subscription_tier VARCHAR,
    age_group VARCHAR
);

-- Load data from S3
COPY INTO raw_viewership
FROM @s3_viewership_stage
FILE_FORMAT = parquet_format
ON_ERROR = 'CONTINUE';

COPY INTO raw_revenue
FROM @s3_revenue_stage
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

COPY INTO raw_content
FROM @s3_content_stage
FILE_FORMAT = json_format
ON_ERROR = 'CONTINUE';

COPY INTO raw_users
FROM @s3_users_stage
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

-- Verify the data loaded
SELECT 'raw_viewership' as table_name, COUNT(*) as row_count FROM raw_viewership
UNION ALL
SELECT 'raw_revenue', COUNT(*) FROM raw_revenue
UNION ALL
SELECT 'raw_content', COUNT(*) FROM raw_content
UNION ALL
SELECT 'raw_users', COUNT(*) FROM raw_users;

-- Check sample data
SELECT * FROM raw_viewership LIMIT 10;
SELECT * FROM raw_revenue LIMIT 10;
SELECT * FROM raw_content LIMIT 10;
SELECT * FROM raw_users LIMIT 10;
