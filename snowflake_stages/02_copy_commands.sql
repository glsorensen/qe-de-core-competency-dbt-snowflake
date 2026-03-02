-- COPY INTO Commands for Loading Data from S3 to Snowflake
-- Supports both initial bulk loads and incremental updates

USE DATABASE MEDIA_ANALYTICS;
USE SCHEMA RAW;
USE WAREHOUSE COMPUTE_WH;  -- Adjust based on your warehouse

/*
========================================
PATTERN 1: SIMPLE BULK LOAD
========================================
Load all files from a stage into a table
*/

-- Load content catalog (full refresh)
COPY INTO RAW.content_catalog
FROM @s3_content_stage
FILE_FORMAT = (FORMAT_NAME = parquet_format)
PATTERN = '.*content_catalog.*\.parquet'
ON_ERROR = 'CONTINUE'
PURGE = FALSE
FORCE = FALSE;

-- Load users (full refresh)
COPY INTO RAW.users
FROM @s3_users_stage
FILE_FORMAT = (FORMAT_NAME = parquet_format)
PATTERN = '.*users.*\.parquet'
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

-- Load subscriptions (full refresh)
COPY INTO RAW.subscriptions
FROM @s3_subscriptions_stage
FILE_FORMAT = (FORMAT_NAME = parquet_format)
PATTERN = '.*subscriptions.*\.parquet'
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

/*
========================================
PATTERN 2: INCREMENTAL LOAD (DATE PARTITIONED)
========================================
Load only new/unloaded files using Snowflake's automatic metadata tracking
*/

-- Load viewership events (incremental)
COPY INTO RAW.viewership_events
FROM @s3_viewership_stage
FILE_FORMAT = (FORMAT_NAME = parquet_format)
PATTERN = '.*viewership_.*\.parquet'
ON_ERROR = 'CONTINUE'
PURGE = FALSE;  -- Keep files in S3 for archive

-- Load revenue transactions (incremental)
COPY INTO RAW.revenue_transactions
FROM @s3_revenue_stage
FILE_FORMAT = (FORMAT_NAME = parquet_format)
PATTERN = '.*revenue_.*\.parquet'
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

/*
========================================
PATTERN 3: DATE-SPECIFIC LOAD
========================================
Load data for specific date partitions
*/

-- Load viewership for specific date
COPY INTO RAW.viewership_events
FROM @s3_viewership_stage/year=2024/month=01/day=01/
FILE_FORMAT = (FORMAT_NAME = parquet_format)
PATTERN = '.*\.parquet'
ON_ERROR = 'CONTINUE';

-- Load viewership for date range (multiple commands or loop in procedure)
COPY INTO RAW.viewership_events
FROM @s3_viewership_stage/year=2024/month=01/
FILE_FORMAT = (FORMAT_NAME = parquet_format)
PATTERN = '.*\.parquet'
ON_ERROR = 'CONTINUE';

/*
========================================
PATTERN 4: LOAD WITH COLUMN TRANSFORMATION
========================================
Apply transformations during load
*/

COPY INTO RAW.viewership_events
FROM (
    SELECT
        $1:event_id::VARCHAR(50) AS event_id,
        $1:user_id::VARCHAR(50) AS user_id,
        $1:content_id::VARCHAR(50) AS content_id,
        $1:view_start_time::TIMESTAMP_NTZ AS view_start_time,
        $1:view_end_time::TIMESTAMP_NTZ AS view_end_time,
        $1:watch_duration_minutes::NUMBER(10,2) AS watch_duration_minutes,
        $1:completion_percentage::NUMBER(5,2) AS completion_percentage,
        $1:device_type::VARCHAR(50) AS device_type,
        $1:platform::VARCHAR(50) AS platform,
        $1:quality::VARCHAR(10) AS quality,
        $1:buffering_events::INTEGER AS buffering_events,
        $1:country::VARCHAR(3) AS country,
        $1:created_at::TIMESTAMP_NTZ AS created_at,
        CURRENT_TIMESTAMP() AS loaded_at
    FROM @s3_viewership_stage
)
FILE_FORMAT = (FORMAT_NAME = parquet_format)
ON_ERROR = 'CONTINUE';

/*
========================================
PATTERN 5: LOAD WITH VALIDATION
========================================
Skip invalid records and log errors
*/

-- Create error tracking table
CREATE TABLE IF NOT EXISTS RAW.load_errors (
    error_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    table_name VARCHAR(100),
    file_name VARCHAR(500),
    row_number INTEGER,
    error_message VARCHAR(5000),
    rejected_record VARIANT
);

-- Load with detailed error handling
COPY INTO RAW.viewership_events
FROM @s3_viewership_stage
FILE_FORMAT = (FORMAT_NAME = parquet_format)
PATTERN = '.*viewership_.*\.parquet'
ON_ERROR = 'CONTINUE'
VALIDATION_MODE = 'RETURN_ERRORS'  -- Use for testing only
PURGE = FALSE;

/*
========================================
PATTERN 6: INCREMENTAL MERGE (UPSERT)
========================================
Load data and merge with existing records
*/

-- Step 1: Load into staging table
CREATE OR REPLACE TEMPORARY TABLE staging_subscriptions LIKE RAW.subscriptions;

COPY INTO staging_subscriptions
FROM @s3_subscriptions_stage
FILE_FORMAT = (FORMAT_NAME = parquet_format)
PATTERN = '.*subscriptions.*\.parquet'
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Step 2: Merge staging into target
MERGE INTO RAW.subscriptions AS target
USING (
    SELECT 
        subscription_id,
        user_id,
        tier,
        price_per_month,
        billing_cycle,
        start_date,
        end_date,
        status,
        auto_renew,
        created_at,
        updated_at,
        CURRENT_TIMESTAMP() AS loaded_at
    FROM staging_subscriptions
) AS source
ON target.subscription_id = source.subscription_id
WHEN MATCHED THEN
    UPDATE SET
        target.status = source.status,
        target.end_date = source.end_date,
        target.auto_renew = source.auto_renew,
        target.updated_at = source.updated_at,
        target.loaded_at = source.loaded_at
WHEN NOT MATCHED THEN
    INSERT (
        subscription_id, user_id, tier, price_per_month, billing_cycle,
        start_date, end_date, status, auto_renew,
        created_at, updated_at, loaded_at
    )
    VALUES (
        source.subscription_id, source.user_id, source.tier, 
        source.price_per_month, source.billing_cycle,
        source.start_date, source.end_date, source.status, source.auto_renew,
        source.created_at, source.updated_at, source.loaded_at
    );

/*
========================================
PATTERN 7: SCHEDULED INCREMENTAL LOAD
========================================
Create task to run incremental loads automatically
*/

-- Create warehouse for scheduled tasks (auto-suspend/resume)
CREATE WAREHOUSE IF NOT EXISTS LOAD_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for scheduled data loads';

-- Task for daily viewership load
CREATE OR REPLACE TASK load_viewership_daily
    WAREHOUSE = LOAD_WH
    SCHEDULE = 'USING CRON 0 2 * * * America/Los_Angeles'  -- 2 AM daily
AS
    COPY INTO RAW.viewership_events
    FROM @s3_viewership_stage
    FILE_FORMAT = (FORMAT_NAME = parquet_format)
    PATTERN = '.*viewership_.*\.parquet'
    ON_ERROR = 'CONTINUE'
    PURGE = FALSE;

-- Task for daily revenue load
CREATE OR REPLACE TASK load_revenue_daily
    WAREHOUSE = LOAD_WH
    SCHEDULE = 'USING CRON 0 2 * * * America/Los_Angeles'
AS
    COPY INTO RAW.revenue_transactions
    FROM @s3_revenue_stage
    FILE_FORMAT = (FORMAT_NAME = parquet_format)
    PATTERN = '.*revenue_.*\.parquet'
    ON_ERROR = 'CONTINUE'
    PURGE = FALSE;

-- Enable tasks
ALTER TASK load_viewership_daily RESUME;
ALTER TASK load_revenue_daily RESUME;

-- Monitor tasks
SHOW TASKS;
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) 
ORDER BY SCHEDULED_TIME DESC 
LIMIT 10;

/*
========================================
MONITORING & VALIDATION QUERIES
========================================
*/

-- Check load history
SELECT
    file_name,
    table_name,
    status,
    row_count,
    row_parsed,
    first_error_message,
    first_error_line_number,
    first_error_character_pos,
    first_error_column_name,
    error_count,
    error_limit,
    load_time
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'MEDIA_ANALYTICS.RAW.VIEWERSHIP_EVENTS',
    START_TIME => DATEADD(days, -7, CURRENT_TIMESTAMP())
))
ORDER BY last_load_time DESC;

-- Check for errors
SELECT 
    table_name,
    COUNT(*) as error_count,
    MIN(error_time) as first_error,
    MAX(error_time) as last_error
FROM RAW.load_errors
GROUP BY table_name
ORDER BY error_count DESC;

-- Verify data freshness
SELECT
    'viewership_events' AS table_name,
    COUNT(*) AS row_count,
    MAX(view_start_time) AS latest_event,
    MAX(loaded_at) AS last_loaded
FROM RAW.viewership_events
UNION ALL
SELECT
    'revenue_transactions' AS table_name,
    COUNT(*) AS row_count,
    MAX(transaction_time) AS latest_event,
    MAX(loaded_at) AS last_loaded
FROM RAW.revenue_transactions
UNION ALL
SELECT
    'content_catalog' AS table_name,
    COUNT(*) AS row_count,
    MAX(updated_at) AS latest_event,
    MAX(loaded_at) AS last_loaded
FROM RAW.content_catalog;

-- Check for duplicates (data quality)
SELECT
    event_id,
    COUNT(*) as duplicate_count
FROM RAW.viewership_events
GROUP BY event_id
HAVING COUNT(*) > 1;

-- Row count by partition date
SELECT
    DATE_TRUNC('day', view_start_time) AS event_date,
    COUNT(*) AS event_count,
    SUM(watch_duration_minutes) AS total_watch_minutes
FROM RAW.viewership_events
GROUP BY event_date
ORDER BY event_date DESC;

/*
========================================
CLEANUP COMMANDS
========================================
*/

-- Suspend tasks
ALTER TASK load_viewership_daily SUSPEND;
ALTER TASK load_revenue_daily SUSPEND;

-- Clear staging/temp tables
TRUNCATE TABLE IF EXISTS staging_subscriptions;

-- Remove files from internal stage (if needed)
REMOVE @s3_viewership_stage PATTERN='.*\.parquet';

/*
========================================
ERROR RECOVERY
========================================
*/

-- Reload specific file that failed
COPY INTO RAW.viewership_events
FROM @s3_viewership_stage
FILES = ('year=2024/month=01/day=01/viewership_20240101_001.parquet')
FILE_FORMAT = (FORMAT_NAME = parquet_format)
ON_ERROR = 'CONTINUE'
FORCE = TRUE;  -- Force reload even if metadata says it's loaded

-- Clear load history metadata (use with caution!)
-- This allows reloading files that Snowflake thinks are already loaded
-- Note: This is a simulated clear; actual method varies
TRUNCATE TABLE RAW.viewership_events;  -- Then reload

/*
========================================
BEST PRACTICES
========================================
1. Use PURGE = FALSE for production (keep source files)
2. Use ON_ERROR = 'CONTINUE' to skip bad records
3. Monitor COPY_HISTORY regularly
4. Set up alerting for failed loads
5. Use appropriate warehouse size (start small, scale up)
6. Leverage Snowflake's metadata tracking (don't reload same files)
7. Use tasks or Snowpipe for automation
8. Implement data quality checks post-load
9. Archive old data with S3 lifecycle policies
10. Tag resources for cost tracking
*/

-- Script complete!
SELECT 'COPY commands ready! Review and execute as needed.' AS status;
