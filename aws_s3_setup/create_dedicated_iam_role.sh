#!/bin/bash
# Create dedicated IAM role for Meagan's Snowflake capstone project
# This follows security best practices with least privilege access

set -e

BUCKET_NAME="qe-de-capstone-mahmed"
ACCOUNT_ID="066396400174"
ROLE_NAME="snowflake-meagan-capstone-role"
POLICY_NAME="snowflake-meagan-capstone-s3-policy"

# From DESC STORAGE INTEGRATION s3_capstone_integration
SNOWFLAKE_IAM_USER_ARN="arn:aws:iam::749449158880:user/etmm0000-s"
SNOWFLAKE_EXTERNAL_ID="KZB56749_SFCRole=144_X+otGhLuF7w="

echo "================================================"
echo "Creating Dedicated IAM Role for Meagan's Capstone"
echo "================================================"
echo "Bucket: ${BUCKET_NAME}"
echo "Role: ${ROLE_NAME}"
echo "Snowflake User: ${SNOWFLAKE_IAM_USER_ARN}"
echo ""
echo "Security Benefits:"
echo "  ✓ Dedicated role (not shared)"
echo "  ✓ Least privilege (only your bucket)"
echo "  ✓ Specific external ID for your integration"
echo "  ✓ Easy to audit and revoke"
echo ""

# Check if AWS credentials are configured
if ! aws sts get-caller-identity &> /dev/null; then
    echo "❌ Error: AWS credentials not configured"
    echo ""
    echo "Please run ONE of these first:"
    echo "  aws configure           # For access keys"
    echo "  aws configure sso       # For SSO"
    echo ""
    exit 1
fi

echo "Step 1: Creating IAM Trust Policy..."
cat > /tmp/trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowSnowflakeAssumeRole",
      "Effect": "Allow",
      "Principal": {
        "AWS": "${SNOWFLAKE_IAM_USER_ARN}"
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

echo "Step 2: Creating IAM role..."
aws iam create-role \
    --role-name ${ROLE_NAME} \
    --assume-role-policy-document file:///tmp/trust-policy.json \
    --description "Dedicated Snowflake role for Meagan's capstone - S3 bucket: ${BUCKET_NAME}" \
    --tags Key=Project,Value=MeaganCapstone Key=Owner,Value=Meagan Key=Purpose,Value=SnowflakeS3Integration \
    2>/dev/null || {
        echo "ℹ️  Role may already exist, updating trust policy..."
        aws iam update-assume-role-policy \
            --role-name ${ROLE_NAME} \
            --policy-document file:///tmp/trust-policy.json
    }

echo "Step 3: Creating S3 Access Policy..."
cat > /tmp/s3-access-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowListBucket",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": "arn:aws:s3:::${BUCKET_NAME}",
      "Condition": {
        "StringLike": {
          "s3:prefix": [
            "raw/*",
            "stage/*"
          ]
        }
      }
    },
    {
      "Sid": "AllowReadWriteObjects",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::${BUCKET_NAME}/raw/*",
        "arn:aws:s3:::${BUCKET_NAME}/stage/*"
      ]
    },
    {
      "Sid": "AllowErrorHandling",
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
    --policy-name ${POLICY_NAME} \
    --policy-document file:///tmp/s3-access-policy.json \
    --description "S3 access for Snowflake - Meagan capstone (least privilege)" \
    --tags Key=Project,Value=MeaganCapstone Key=Owner,Value=Meagan \
    2>/dev/null || echo "ℹ️  Policy may already exist"

echo "Step 4: Attaching policy to role..."
aws iam attach-role-policy \
    --role-name ${ROLE_NAME} \
    --policy-arn "arn:aws:iam::${ACCOUNT_ID}:policy/${POLICY_NAME}"

# Get role ARN
ROLE_ARN=$(aws iam get-role --role-name ${ROLE_NAME} --query 'Role.Arn' --output text)

# Clean up temp files
rm -f /tmp/trust-policy.json /tmp/s3-access-policy.json

echo ""
echo "================================================"
echo "✅ Dedicated IAM Role Created Successfully!"
echo "================================================"
echo ""
echo "Role ARN: ${ROLE_ARN}"
echo ""
echo "Security Features:"
echo "  ✓ Unique to your capstone project"
echo "  ✓ Only allows access to: ${BUCKET_NAME}"
echo "  ✓ Limited to raw/ and stage/ folders"
echo "  ✓ Requires specific External ID"
echo "  ✓ No access to other S3 buckets"
echo ""
echo "================================================"
echo "NEXT STEP: Update Snowflake Storage Integration"
echo "================================================"
echo ""
echo "In Snowflake, run:"
echo ""
echo "USE ROLE SNOWFLAKE_ILABS_QECATALYST;"
echo "USE DATABASE MEDIA_ANALYTICS;"
echo ""
echo "-- Drop old integration"
echo "DROP STORAGE INTEGRATION IF EXISTS s3_capstone_integration;"
echo ""
echo "-- Create new with dedicated role"
echo "CREATE STORAGE INTEGRATION s3_capstone_integration"
echo "  TYPE = EXTERNAL_STAGE"
echo "  STORAGE_PROVIDER = 'S3'"
echo "  ENABLED = TRUE"
echo "  STORAGE_AWS_ROLE_ARN = '${ROLE_ARN}'"
echo "  STORAGE_ALLOWED_LOCATIONS = ('s3://${BUCKET_NAME}/')"
echo "  COMMENT = 'Meagan capstone - dedicated S3 integration';"
echo ""
echo "-- Verify it worked"
echo "DESC STORAGE INTEGRATION s3_capstone_integration;"
echo ""
echo "================================================"
