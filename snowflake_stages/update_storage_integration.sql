-- ================================================
-- Update Storage Integration with New IAM Role
-- ================================================

USE ROLE SNOWFLAKE_ILABS_QECATALYST;
USE DATABASE MEDIA_ANALYTICS;

-- Update the storage integration with the new IAM role ARN
ALTER STORAGE INTEGRATION s3_capstone_integration
  SET STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::066396400174:role/snowflake-meagan-capstone-role';

-- Verify the update
DESC STORAGE INTEGRATION s3_capstone_integration;

-- Test the connection by creating an external stage
CREATE OR REPLACE STAGE s3_test_stage
  URL = 's3://qe-de-capstone-mahmed/'
  STORAGE_INTEGRATION = s3_capstone_integration;

-- List contents (should work if connection is successful)
LIST @s3_test_stage;

-- If that works, create the permanent stages
CREATE OR REPLACE STAGE s3_raw_stage
  URL = 's3://qe-de-capstone-mahmed/raw/'
  STORAGE_INTEGRATION = s3_capstone_integration;

CREATE OR REPLACE STAGE s3_stage_stage
  URL = 's3://qe-de-capstone-mahmed/stage/'
  STORAGE_INTEGRATION = s3_capstone_integration;

-- Test listing the stages
LIST @s3_raw_stage;
LIST @s3_stage_stage;

SHOW STAGES;
