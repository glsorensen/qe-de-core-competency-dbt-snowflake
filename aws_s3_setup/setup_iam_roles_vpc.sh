#!/bin/bash
# Create IAM role for Snowflake S3 access using VPC endpoint authentication
# This is more secure than using IAM user credentials

set -e

# Load VPC configuration
if [[ -f /tmp/snowflake_vpc_config.env ]]; then
    source /tmp/snowflake_vpc_config.env
    echo "Loaded VPC configuration"
else
    echo "Error: VPC configuration not found"
    echo "Please run ./get_snowflake_vpc_ids.sh first"
    exit 1
fi

# AWS Configuration
ROLE_NAME="snowflake-s3-integration-role-meagan"
POLICY_NAME="snowflake-s3-access-policy-meagan"
PROFILE="default"

echo "================================================"
echo "Creating IAM Role for Snowflake (VPC-based)"
echo "================================================"
echo ""

# NOTE: You need to get these values from Snowflake first!
echo "⚠️  IMPORTANT: Before running this script, you need:"
echo "1. Your Snowflake AWS Account ID"
echo "2. Your Snowflake External ID"
echo ""
echo "To get these values, run in Snowflake:"
echo "  DESC STORAGE INTEGRATION <integration_name>;"
echo ""
echo "After creating the storage integration in Snowflake,"
echo "you'll get these values to update the trust policy."
echo ""

read -p "Do you have the Snowflake AWS Account ID and External ID? (yes/no): " has_snowflake_info

if [[ "$has_snowflake_info" != "yes" ]]; then
    echo ""
    echo "Please complete these steps first:"
    echo "1. Create storage integration in Snowflake (see snowflake_stages/01_create_stages_vpc.sql)"
    echo "2. Run: DESC STORAGE INTEGRATION s3_media_analytics_integration;"
    echo "3. Record STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID"
    echo "4. Re-run this script"
    echo ""
    exit 0
fi

echo ""
read -p "Enter Snowflake AWS Account ID (from DESC STORAGE INTEGRATION): " SNOWFLAKE_AWS_ACCOUNT_ID
read -p "Enter Snowflake External ID (from DESC STORAGE INTEGRATION): " SNOWFLAKE_EXTERNAL_ID

if [[ -z "$SNOWFLAKE_AWS_ACCOUNT_ID" ]] || [[ -z "$SNOWFLAKE_EXTERNAL_ID" ]]; then
    echo "Error: Both Snowflake AWS Account ID and External ID are required"
    exit 1
fi

echo ""
echo "Configuration:"
echo "  IAM Role: ${ROLE_NAME}"
echo "  S3 Bucket: ${BUCKET_NAME}"
echo "  Snowflake VPC: ${SNOWFLAKE_VPC_ID}"
echo "  Snowflake Account: ${SNOWFLAKE_AWS_ACCOUNT_ID}"
echo ""

# Create trust policy document
cat > /tmp/trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::${SNOWFLAKE_AWS_ACCOUNT_ID}:user/snowflake"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "${SNOWFLAKE_EXTERNAL_ID}"
        }
      }
    }
  ]
}
EOF

# Create the role
echo "Creating IAM role: ${ROLE_NAME}..."
aws iam create-role \
    --role-name ${ROLE_NAME} \
    --assume-role-policy-document file:///tmp/trust-policy.json \
    --description "Allows Snowflake VPC to access S3 bucket for Meagan's capstone project" \
    --profile ${PROFILE}

# Create access policy (read and write for Snowflake)
echo "Creating IAM policy: ${POLICY_NAME}..."
cat > /tmp/access-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SnowflakeS3ReadWrite",
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
        "arn:aws:s3:::${BUCKET_NAME}",
        "arn:aws:s3:::${BUCKET_NAME}/*"
      ],
      "Condition": {
        "StringEquals": {
          "aws:SourceVpc": "${SNOWFLAKE_VPC_ID}"
        }
      }
    },
    {
      "Sid": "SnowflakeListBuckets",
      "Effect": "Allow",
      "Action": [
        "s3:ListAllMyBuckets",
        "s3:GetBucketLocation"
      ],
      "Resource": "*"
    }
  ]
}
EOF

aws iam create-policy \
    --policy-name ${POLICY_NAME} \
    --policy-document file:///tmp/access-policy.json \
    --description "S3 access policy for Snowflake VPC - Meagan capstone" \
    --profile ${PROFILE}

# Attach policy to role
echo "Attaching policy to role..."
aws iam attach-role-policy \
    --role-name ${ROLE_NAME} \
    --policy-arn "arn:aws:iam::${ACCOUNT_ID}:policy/${POLICY_NAME}" \
    --profile ${PROFILE}

# Get role ARN
ROLE_ARN=$(aws iam get-role --role-name ${ROLE_NAME} --query 'Role.Arn' --output text --profile ${PROFILE})

# Save configuration for Snowflake
cat > /tmp/snowflake_integration_config.txt <<EOF
================================================
Snowflake Storage Integration Configuration
================================================

Use this information in Snowflake:

CREATE STORAGE INTEGRATION s3_media_analytics_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '${ROLE_ARN}'
  STORAGE_ALLOWED_LOCATIONS = (
    's3://${BUCKET_NAME}/raw/',
    's3://${BUCKET_NAME}/stage/'
  )
  STORAGE_BLOCKED_LOCATIONS = (
    's3://${BUCKET_NAME}/errors/',
    's3://${BUCKET_NAME}/archive/'
  )
  COMMENT = 'Meagan capstone - VPC-based S3 integration';

After creating, run:
DESC STORAGE INTEGRATION s3_media_analytics_integration;

================================================
VPC Configuration Summary
================================================
Snowflake VPC ID: ${SNOWFLAKE_VPC_ID}
S3 Bucket: ${BUCKET_NAME}
IAM Role ARN: ${ROLE_ARN}
AWS Region: ${REGION}

================================================
EOF

# Clean up temp files
rm /tmp/trust-policy.json /tmp/access-policy.json

echo ""
echo "================================================"
echo "✅ IAM Role Created Successfully!"
echo "================================================"
echo "Role ARN: ${ROLE_ARN}"
echo ""
echo "Configuration saved to: /tmp/snowflake_integration_config.txt"
echo ""
cat /tmp/snowflake_integration_config.txt
echo ""
echo "Next steps:"
echo "1. Copy the CREATE STORAGE INTEGRATION SQL above"
echo "2. Execute it in Snowflake"
echo "3. Run: DESC STORAGE INTEGRATION s3_media_analytics_integration;"
echo "4. Verify the integration works"
echo "5. Create external stages (see snowflake_stages/01_create_stages_vpc.sql)"
echo "================================================"
