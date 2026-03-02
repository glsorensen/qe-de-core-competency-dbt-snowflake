#!/bin/bash
# Create IAM role for Snowflake to access S3 bucket: qe-de-capstone-mahmed
# Based on storage integration details from Snowflake

set -e

BUCKET_NAME="qe-de-capstone-mahmed"
ACCOUNT_ID="066396400174"
ROLE_NAME="snowflake-capstone-role"
POLICY_NAME="snowflake-capstone-policy"
PROFILE="default"

# These values come from DESC STORAGE INTEGRATION in Snowflake
SNOWFLAKE_IAM_USER_ARN="arn:aws:iam::749449158880:user/etmm0000-s"
SNOWFLAKE_EXTERNAL_ID="KZB56749_SFCRole=144_X+otGhLuF7w="

echo "================================================"
echo "Creating IAM Role for Snowflake S3 Access"
echo "================================================"
echo "Bucket: ${BUCKET_NAME}"
echo "Role: ${ROLE_NAME}"
echo ""

# Create trust policy
cat > /tmp/trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
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

echo "Creating IAM role..."
aws iam create-role \
    --role-name ${ROLE_NAME} \
    --assume-role-policy-document file:///tmp/trust-policy.json \
    --description "Snowflake access to S3 for Meagan capstone" \
    --profile ${PROFILE} || echo "Role may already exist"

# Create access policy
cat > /tmp/access-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
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

echo "Creating IAM policy..."
aws iam create-policy \
    --policy-name ${POLICY_NAME} \
    --policy-document file:///tmp/access-policy.json \
    --description "S3 access for Snowflake - Meagan capstone" \
    --profile ${PROFILE} || echo "Policy may already exist"

echo "Attaching policy to role..."
aws iam attach-role-policy \
    --role-name ${ROLE_NAME} \
    --policy-arn "arn:aws:iam::${ACCOUNT_ID}:policy/${POLICY_NAME}" \
    --profile ${PROFILE}

# Get role ARN
ROLE_ARN=$(aws iam get-role --role-name ${ROLE_NAME} --query 'Role.Arn' --output text --profile ${PROFILE})

# Clean up
rm /tmp/trust-policy.json /tmp/access-policy.json

echo ""
echo "================================================"
echo "✅ IAM Role Created Successfully!"
echo "================================================"
echo "Role ARN: ${ROLE_ARN}"
echo ""
echo "Next steps:"
echo "1. Go back to Snowflake and create external stages"
echo "2. Test the connection with: LIST @stage_name;"
echo "================================================"
