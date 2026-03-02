#!/bin/bash
# Create IAM roles and policies for Snowflake S3 integration
# AWS Account: 066396400174

set -e

# Configuration
ACCOUNT_ID="066396400174"
BUCKET_NAME="snowflake-media-analytics-meagan-${ACCOUNT_ID}"
ROLE_NAME="snowflake-s3-integration-role-meagan"
READ_POLICY_NAME="snowflake-s3-read-policy-meagan"
WRITE_POLICY_NAME="snowflake-s3-write-policy-meagan"
PROFILE="default"

echo "================================================"
echo "Creating IAM Roles for Snowflake Integration"
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
echo "Please update the variables below:"
echo ""

# TODO: Update these with your Snowflake values
SNOWFLAKE_AWS_ACCOUNT_ID="<SNOWFLAKE_AWS_ACCOUNT_ID>"
SNOWFLAKE_EXTERNAL_ID="<SNOWFLAKE_EXTERNAL_ID>"

if [ "${SNOWFLAKE_AWS_ACCOUNT_ID}" == "<SNOWFLAKE_AWS_ACCOUNT_ID>" ]; then
    echo "❌ Error: Please update SNOWFLAKE_AWS_ACCOUNT_ID in this script"
    echo "   Run this in Snowflake after creating storage integration:"
    echo "   DESC STORAGE INTEGRATION s3_media_analytics_integration;"
    exit 1
fi

echo "Creating IAM role: ${ROLE_NAME}..."

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
aws iam create-role \
    --role-name ${ROLE_NAME} \
    --assume-role-policy-document file:///tmp/trust-policy.json \
    --description "Allows Snowflake to access S3 bucket for analytics pipeline" \
    --profile ${PROFILE}

# Create read policy
echo "Creating read policy: ${READ_POLICY_NAME}..."
cat > /tmp/read-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::${BUCKET_NAME}",
        "arn:aws:s3:::${BUCKET_NAME}/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListAllMyBuckets"
      ],
      "Resource": "*"
    }
  ]
}
EOF

aws iam create-policy \
    --policy-name ${READ_POLICY_NAME} \
    --policy-document file:///tmp/read-policy.json \
    --description "Read-only access to Snowflake media analytics S3 bucket" \
    --profile ${PROFILE}

# Create write policy (for error files)
echo "Creating write policy: ${WRITE_POLICY_NAME}..."
cat > /tmp/write-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::${BUCKET_NAME}/errors/*",
        "arn:aws:s3:::${BUCKET_NAME}/rejected/*"
      ]
    }
  ]
}
EOF

aws iam create-policy \
    --policy-name ${WRITE_POLICY_NAME} \
    --policy-document file:///tmp/write-policy.json \
    --description "Write access to error/rejected folders in S3" \
    --profile ${PROFILE}

# Attach policies to role
echo "Attaching policies to role..."
aws iam attach-role-policy \
    --role-name ${ROLE_NAME} \
    --policy-arn "arn:aws:iam::${ACCOUNT_ID}:policy/${READ_POLICY_NAME}" \
    --profile ${PROFILE}

aws iam attach-role-policy \
    --role-name ${ROLE_NAME} \
    --policy-arn "arn:aws:iam::${ACCOUNT_ID}:policy/${WRITE_POLICY_NAME}" \
    --profile ${PROFILE}

# Get role ARN
ROLE_ARN=$(aws iam get-role --role-name ${ROLE_NAME} --query 'Role.Arn' --output text --profile ${PROFILE})

# Clean up temp files
rm /tmp/trust-policy.json /tmp/read-policy.json /tmp/write-policy.json

echo ""
echo "================================================"
echo "✅ IAM Roles Created Successfully!"
echo "================================================"
echo "Role ARN: ${ROLE_ARN}"
echo ""
echo "Next steps:"
echo "1. Copy the Role ARN above"
echo "2. Use it in Snowflake storage integration:"
echo ""
echo "CREATE STORAGE INTEGRATION s3_media_analytics_integration"
echo "  TYPE = EXTERNAL_STAGE"
echo "  STORAGE_PROVIDER = 'S3'"
echo "  ENABLED = TRUE"
echo "  STORAGE_AWS_ROLE_ARN = '${ROLE_ARN}'"
echo "  STORAGE_ALLOWED_LOCATIONS = ('s3://${BUCKET_NAME}/raw/', 's3://${BUCKET_NAME}/stage/');"
echo ""
echo "3. After creating storage integration, run:"
echo "   DESC STORAGE INTEGRATION s3_media_analytics_integration;"
echo "4. Update this script with STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID"
echo "5. Re-run to update trust policy if needed"
echo "================================================"
