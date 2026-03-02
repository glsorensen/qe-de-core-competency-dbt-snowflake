#!/bin/bash
# Get Snowflake VPC IDs for secure S3 access configuration
# This script helps you retrieve the VPC IDs needed for S3 bucket policies

set -e

echo "================================================"
echo "Snowflake VPC ID Retrieval Guide"
echo "================================================"
echo ""
echo "This guide will help you set up VPC-based S3 access for Snowflake."
echo "This is more secure than using IAM user credentials."
echo ""
echo "STEP 1: Get Snowflake VPC IDs"
echo "================================================"
echo ""
echo "Log into Snowflake and run the following SQL:"
echo ""
echo "-- Set role to ACCOUNTADMIN"
echo "USE ROLE ACCOUNTADMIN;"
echo ""
echo "-- Get VPC information"
echo "SELECT SYSTEM\$GET_SNOWFLAKE_PLATFORM_INFO();"
echo ""
echo "The output will be JSON containing:"
echo "  - snowflakeVpcId: The VPC ID(s) for your Snowflake account"
echo "  - awsRegion: The AWS region of your Snowflake account"
echo ""
echo "Example output:"
echo '{'
echo '  "snowflakeVpcId": ["vpc-1234567890abcdef0"],'
echo '  "awsRegion": "us-east-1"'
echo '}'
echo ""
echo "================================================"
echo "STEP 2: Record Your VPC IDs"
echo "================================================"
echo ""
echo "Copy the VPC ID(s) from the query result above."
echo "You will need this for the S3 bucket policy."
echo ""
echo "Save it here for reference:"
echo "SNOWFLAKE_VPC_ID=\"vpc-XXXXXXXXXXXXXXX\""
echo ""
echo "================================================"
echo "STEP 3: Continue with this script"
echo "================================================"
echo ""
read -p "Have you retrieved your Snowflake VPC ID? (yes/no): " response

if [[ "$response" != "yes" ]]; then
    echo ""
    echo "Please complete Steps 1-2 first, then re-run this script."
    echo ""
    exit 0
fi

echo ""
read -p "Enter your Snowflake VPC ID (e.g., vpc-1234567890abcdef0): " SNOWFLAKE_VPC_ID

if [[ -z "$SNOWFLAKE_VPC_ID" ]]; then
    echo "Error: VPC ID is required"
    exit 1
fi

# Configuration
ACCOUNT_ID="066396400174"
BUCKET_NAME="snowflake-media-analytics-meagan-${ACCOUNT_ID}"
REGION="us-east-1"

echo ""
echo "Configuration:"
echo "  Snowflake VPC ID: ${SNOWFLAKE_VPC_ID}"
echo "  S3 Bucket: ${BUCKET_NAME}"
echo "  AWS Region: ${REGION}"
echo ""

# Save VPC ID to file for later use
cat > /tmp/snowflake_vpc_config.env <<EOF
SNOWFLAKE_VPC_ID="${SNOWFLAKE_VPC_ID}"
BUCKET_NAME="${BUCKET_NAME}"
ACCOUNT_ID="${ACCOUNT_ID}"
REGION="${REGION}"
EOF

echo "✅ Configuration saved to /tmp/snowflake_vpc_config.env"
echo ""
echo "================================================"
echo "NEXT STEPS:"
echo "================================================"
echo "1. Run: ./create_s3_bucket_vpc.sh"
echo "   This will create S3 buckets with VPC endpoint policies"
echo ""
echo "2. Run: ./setup_iam_roles_vpc.sh"
echo "   This will create IAM role for Snowflake with VPC access"
echo ""
echo "3. Configure Snowflake storage integration (see snowflake_stages/01_create_stages.sql)"
echo "================================================"
