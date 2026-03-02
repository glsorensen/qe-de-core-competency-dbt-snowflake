-- Snowpipe Setup for Continuous S3 to Snowflake Ingestion
-- Automatically loads data as files arrive in S3 using event notifications

USE DATABASE MEDIA_ANALYTICS;
USE SCHEMA RAW;

/*
========================================
OVERVIEW: SNOWPIPE ARCHITECTURE
========================================
1. Files land in S3
2. S3 Event Notification triggers SNS/SQS
3. Snowpipe receives notification
4. Snowpipe loads data automatically (serverless)
5. No warehouse required (Snowflake-managed compute)

Benefits:
- Near real-time ingestion
- No manual scheduling
- Cost-effective (pay per file loaded)
- Automatic retry on failure
*/

/*
========================================
STEP 1: CREATE NOTIFICATION INTEGRATION
========================================
Allows Snowflake to receive S3 event notifications
*/

CREATE OR REPLACE NOTIFICATION INTEGRATION s3_sns_notification_integration
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AWS_SNS
  ENABLED = TRUE
  AWS_SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:066396400174:snowflake-media-analytics-meagan-notifications'
  AWS_SNS_ROLE_ARN = 'arn:aws:iam::066396400174:role/snowflake-sns-role-meagan'
  COMMENT = 'SNS integration for S3 event notifications - Meagan capstone project';

-- View integration details (needed for AWS SNS trust policy)
DESC NOTIFICATION INTEGRATION s3_sns_notification_integration;

-- Grant usage
GRANT USAGE ON INTEGRATION s3_sns_notification_integration TO ROLE DATA_ENGINEER;

/*
========================================
AWS SETUP REQUIRED (Before enabling pipes)
========================================

1. Create SNS Topic:
   - Name: snowflake-media-analytics-meagan-notifications
   - Type: Standard

2. Create SQS Queue:
   - Name: snowflake-media-analytics-meagan-queue
   - Type: Standard
   - Subscribe to SNS topic

3. Configure S3 Event Notifications:
   - Event types: s3:ObjectCreated:*
   - Prefix filters by folder (raw/viewership/, raw/revenue/, etc.)
   - Send to SNS topic

4. Update SNS/SQS Access Policies:
   - Allow Snowflake AWS user (from DESC NOTIFICATION INTEGRATION)
   - Allow S3 to publish to SNS

See aws_s3_setup/setup_snowpipe_notifications.sh for automation
*/

/*
========================================
STEP 2: CREATE SNOWPIPES
========================================
*/

-- Snowpipe for viewership events
CREATE OR REPLACE PIPE pipe_viewership_events
  AUTO_INGEST = TRUE
  AWS_SNS_TOPIC = 'arn:aws:sns:us-east-1:066396400174:snowflake-media-analytics-meagan-notifications'
  COMMENT = 'Auto-ingest viewership events from S3'
AS
  COPY INTO RAW.viewership_events
  FROM @s3_viewership_stage
  FILE_FORMAT = (FORMAT_NAME = parquet_format)
  PATTERN = '.*viewership_.*\.parquet'
  ON_ERROR = 'CONTINUE'
  PURGE = FALSE;

-- Snowpipe for revenue transactions
CREATE OR REPLACE PIPE pipe_revenue_transactions
  AUTO_INGEST = TRUE
  AWS_SNS_TOPIC = 'arn:aws:sns:us-east-1:066396400174:snowflake-media-analytics-meagan-notifications'
  COMMENT = 'Auto-ingest revenue transactions from S3'
AS
  COPY INTO RAW.revenue_transactions
  FROM @s3_revenue_stage
  FILE_FORMAT = (FORMAT_NAME = parquet_format)
  PATTERN = '.*revenue_.*\.parquet'
  ON_ERROR = 'CONTINUE'
  PURGE = FALSE;

-- Snowpipe for content updates (less frequent)
CREATE OR REPLACE PIPE pipe_content_catalog
  AUTO_INGEST = TRUE
  AWS_SNS_TOPIC = 'arn:aws:sns:us-east-1:066396400174:snowflake-media-analytics-meagan-notifications'
  COMMENT = 'Auto-ingest content catalog updates'
AS
  COPY INTO RAW.content_catalog
  FROM @s3_content_stage
  FILE_FORMAT = (FORMAT_NAME = parquet_format)
  PATTERN = '.*content_catalog.*\.parquet'
  ON_ERROR = 'CONTINUE'
  PURGE = FALSE;

-- Snowpipe for user updates
CREATE OR REPLACE PIPE pipe_users
  AUTO_INGEST = TRUE
  AWS_SNS_TOPIC = 'arn:aws:sns:us-east-1:066396400174:snowflake-media-analytics-meagan-notifications'
  COMMENT = 'Auto-ingest user profile updates'
AS
  COPY INTO RAW.users
  FROM @s3_users_stage
  FILE_FORMAT = (FORMAT_NAME = parquet_format)
  PATTERN = '.*users.*\.parquet'
  ON_ERROR = 'CONTINUE'
  PURGE = FALSE;

-- Snowpipe for subscriptions
CREATE OR REPLACE PIPE pipe_subscriptions
  AUTO_INGEST = TRUE
  AWS_SNS_TOPIC = 'arn:aws:sns:us-east-1:066396400174:snowflake-media-analytics-meagan-notifications'
  COMMENT = 'Auto-ingest subscription updates'
AS
  COPY INTO RAW.subscriptions
  FROM @s3_subscriptions_stage
  FILE_FORMAT = (FORMAT_NAME = parquet_format)
  PATTERN = '.*subscriptions.*\.parquet'
  ON_ERROR = 'CONTINUE'
  PURGE = FALSE;

/*
========================================
STEP 3: GRANT PRIVILEGES
========================================
*/

GRANT OWNERSHIP ON PIPE pipe_viewership_events TO ROLE DATA_ENGINEER COPY CURRENT GRANTS;
GRANT OWNERSHIP ON PIPE pipe_revenue_transactions TO ROLE DATA_ENGINEER COPY CURRENT GRANTS;
GRANT OWNERSHIP ON PIPE pipe_content_catalog TO ROLE DATA_ENGINEER COPY CURRENT GRANTS;
GRANT OWNERSHIP ON PIPE pipe_users TO ROLE DATA_ENGINEER COPY CURRENT GRANTS;
GRANT OWNERSHIP ON PIPE pipe_subscriptions TO ROLE DATA_ENGINEER COPY CURRENT GRANTS;

GRANT MONITOR ON PIPE pipe_viewership_events TO ROLE DATA_ANALYST;
GRANT MONITOR ON PIPE pipe_revenue_transactions TO ROLE DATA_ANALYST;

/*
========================================
STEP 4: MONITORING QUERIES
========================================
*/

-- Show all pipes
SHOW PIPES;

-- Pipe status and configuration
DESC PIPE pipe_viewership_events;

-- Check pipe execution history (last 14 days)
SELECT
    PIPE_NAME,
    FILE_NAME,
    STAGE_LOCATION,
    STATUS,
    ROW_COUNT,
    ROW_PARSED,
    FIRST_ERROR_MESSAGE,
    FIRST_ERROR_LINE_NUMBER,
    FIRST_ERROR_CHARACTER_POS,
    ERROR_COUNT,
    ERROR_LIMIT,
    LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'MEDIA_ANALYTICS.RAW.VIEWERSHIP_EVENTS',
    START_TIME => DATEADD(days, -14, CURRENT_TIMESTAMP())
))
WHERE PIPE_NAME IS NOT NULL
ORDER BY LAST_LOAD_TIME DESC;

-- Pipe load summary by day
SELECT
    PIPE_NAME,
    DATE_TRUNC('day', LAST_LOAD_TIME) AS load_date,
    COUNT(*) AS files_loaded,
    SUM(ROW_COUNT) AS total_rows,
    SUM(CASE WHEN STATUS = 'LOADED' THEN 1 ELSE 0 END) AS successful_loads,
    SUM(CASE WHEN STATUS != 'LOADED' THEN 1 ELSE 0 END) AS failed_loads
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'MEDIA_ANALYTICS.RAW.VIEWERSHIP_EVENTS',
    START_TIME => DATEADD(days, -7, CURRENT_TIMESTAMP())
))
WHERE PIPE_NAME IS NOT NULL
GROUP BY PIPE_NAME, load_date
ORDER BY load_date DESC, PIPE_NAME;

-- Check for errors in the last 24 hours
SELECT
    PIPE_NAME,
    FILE_NAME,
    FIRST_ERROR_MESSAGE,
    FIRST_ERROR_LINE_NUMBER,
    ERROR_COUNT,
    LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'MEDIA_ANALYTICS.RAW.VIEWERSHIP_EVENTS',
    START_TIME => DATEADD(hours, -24, CURRENT_TIMESTAMP())
))
WHERE PIPE_NAME IS NOT NULL
  AND STATUS != 'LOADED'
ORDER BY LOAD_TIME DESC;

-- Pipe credit usage (cost monitoring)
SELECT
    PIPE_NAME,
    DATE_TRUNC('day', START_TIME) AS usage_date,
    SUM(CREDITS_USED) AS total_credits,
    COUNT(DISTINCT FILE_NAME) AS files_processed
FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
    START_TIME => DATEADD(days, -30, CURRENT_TIMESTAMP())
))
GROUP BY PIPE_NAME, usage_date
ORDER BY usage_date DESC, total_credits DESC;

-- Current pipe status
SELECT
    SYSTEM$PIPE_STATUS('pipe_viewership_events') AS viewership_status,
    SYSTEM$PIPE_STATUS('pipe_revenue_transactions') AS revenue_status,
    SYSTEM$PIPE_STATUS('pipe_content_catalog') AS content_status;

/*
========================================
STEP 5: MANUAL PIPE OPERATIONS
========================================
*/

-- Manually trigger pipe for existing files (initial backfill)
ALTER PIPE pipe_viewership_events REFRESH;
ALTER PIPE pipe_revenue_transactions REFRESH;

-- Refresh with specific prefix
ALTER PIPE pipe_viewership_events REFRESH 
  PREFIX = 'raw/viewership/year=2024/month=01/';

-- Pause pipe (stop processing)
ALTER PIPE pipe_viewership_events SET PIPE_EXECUTION_PAUSED = TRUE;

-- Resume pipe
ALTER PIPE pipe_viewership_events SET PIPE_EXECUTION_PAUSED = FALSE;

-- Get notification channel for pipe
SELECT SYSTEM$PIPE_NOTIFICATION_CHANNEL('pipe_viewership_events');

/*
========================================
STEP 6: ERROR HANDLING & RECOVERY
========================================
*/

-- Create error tracking table
CREATE TABLE IF NOT EXISTS RAW.pipe_load_errors (
    error_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    pipe_name VARCHAR(100),
    file_name VARCHAR(500),
    error_message VARCHAR(5000),
    row_count INTEGER,
    error_count INTEGER,
    details VARIANT
);

-- Insert errors from copy history
INSERT INTO RAW.pipe_load_errors
SELECT
    LAST_LOAD_TIME AS error_time,
    PIPE_NAME,
    FILE_NAME,
    FIRST_ERROR_MESSAGE,
    ROW_COUNT,
    ERROR_COUNT,
    OBJECT_CONSTRUCT(
        'status', STATUS,
        'error_line', FIRST_ERROR_LINE_NUMBER,
        'error_position', FIRST_ERROR_CHARACTER_POS,
        'error_column', FIRST_ERROR_COLUMN_NAME
    ) AS details
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'MEDIA_ANALYTICS.RAW.VIEWERSHIP_EVENTS',
    START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
))
WHERE PIPE_NAME IS NOT NULL
  AND STATUS != 'LOADED';

-- Reprocess failed files (after fixing issues)
ALTER PIPE pipe_viewership_events REFRESH 
  PREFIX = 'raw/viewership/year=2024/month=01/day=15/'
  MODIFIED_AFTER = '2024-01-15 00:00:00';

/*
========================================
STEP 7: ALERTING & NOTIFICATIONS
========================================
*/

-- Create alert for pipe failures
CREATE OR REPLACE ALERT alert_pipe_failures
  WAREHOUSE = LOAD_WH
  SCHEDULE = '5 MINUTE'
  IF (EXISTS (
    SELECT 1
    FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'MEDIA_ANALYTICS.RAW.VIEWERSHIP_EVENTS',
        START_TIME => DATEADD(minutes, -10, CURRENT_TIMESTAMP())
    ))
    WHERE PIPE_NAME IS NOT NULL
      AND STATUS != 'LOADED'
  ))
  THEN CALL send_notification_procedure('Snowpipe failure detected!');

-- Resume alert
ALTER ALERT alert_pipe_failures RESUME;

/*
========================================
STEP 8: PERFORMANCE OPTIMIZATION
========================================
*/

-- Cluster tables for better query performance
ALTER TABLE RAW.viewership_events 
  CLUSTER BY (TO_DATE(view_start_time));

ALTER TABLE RAW.revenue_transactions 
  CLUSTER BY (TO_DATE(transaction_time));

-- Create streams for CDC (Change Data Capture)
CREATE OR REPLACE STREAM stream_viewership_events 
  ON TABLE RAW.viewership_events
  COMMENT = 'Track changes to viewership events for downstream processing';

CREATE OR REPLACE STREAM stream_revenue_transactions 
  ON TABLE RAW.revenue_transactions
  COMMENT = 'Track changes to revenue transactions';

/*
========================================
BEST PRACTICES
========================================
1. Use Snowpipe for continuous, small-file ingestion
2. Use scheduled COPY for large batch loads
3. Monitor credit usage (Snowpipe can be more expensive)
4. Set up error alerts
5. Use file size 100MB-250MB for optimal performance
6. Implement retry logic for failed files
7. Archive S3 files after successful load
8. Use clustering for frequently queried columns
9. Monitor pipe lag with SYSTEM$PIPE_STATUS
10. Test with small dataset before production
========================================
*/

-- Verification query
SELECT 
    'Snowpipe setup complete!' AS status,
    COUNT(*) AS pipes_created
FROM INFORMATION_SCHEMA.PIPES
WHERE PIPE_SCHEMA = 'RAW';
