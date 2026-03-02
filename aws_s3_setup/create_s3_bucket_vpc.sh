#!/bin/bash
# Create S3 buckets with VPC endpoint policies for Snowflake
# This script creates buckets that only allow access from Snowflake VPCs

set -e

# Load VPC configuration if it exists
if [[ -f /tmp/snowflake_vpc_config.env ]]; then
    source /tmp/snowflake_vpc_config.env
    echo "Loaded VPC configuration from previous step"
else
    echo "Error: VPC configuration not found"
    echo "Please run ./get_snowflake_vpc_ids.sh first"
    exit 1
fi

# AWS Configuration
PROFILE="default"   # Update if using named AWS profile

echo "================================================"
echo "Creating S3 Infrastructure with VPC Endpoint Policy"
echo "================================================"
echo "Account ID: ${ACCOUNT_ID}"
echo "Region: ${REGION}"
echo "Bucket: ${BUCKET_NAME}"
echo "Snowflake VPC: ${SNOWFLAKE_VPC_ID}"
echo ""

# Check AWS CLI is configured
if ! command -v aws &> /dev/null; then
    echo "Error: AWS CLI is not installed"
    exit 1
fi

# Verify AWS credentials
echo "Verifying AWS credentials..."
aws sts get-caller-identity --profile ${PROFILE}

LOGS_BUCKET_NAME="${BUCKET_NAME}-logs"

# Create logs bucket first
echo ""
echo "Creating logs bucket: ${LOGS_BUCKET_NAME}..."
if [ "${REGION}" == "us-east-1" ]; then
    aws s3api create-bucket \
        --bucket ${LOGS_BUCKET_NAME} \
        --profile ${PROFILE}
else
    aws s3api create-bucket \
        --bucket ${LOGS_BUCKET_NAME} \
        --region ${REGION} \
        --create-bucket-configuration LocationConstraint=${REGION} \
        --profile ${PROFILE}
fi

# Enable versioning on logs bucket
echo "Enabling versioning on logs bucket..."
aws s3api put-bucket-versioning \
    --bucket ${LOGS_BUCKET_NAME} \
    --versioning-configuration Status=Enabled \
    --profile ${PROFILE}

# Block public access on logs bucket
echo "Blocking public access on logs bucket..."
aws s3api put-public-access-block \
    --bucket ${LOGS_BUCKET_NAME} \
    --public-access-block-configuration \
        "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true" \
    --profile ${PROFILE}

# Create main data bucket
echo ""
echo "Creating main data bucket: ${BUCKET_NAME}..."
if [ "${REGION}" == "us-east-1" ]; then
    aws s3api create-bucket \
        --bucket ${BUCKET_NAME} \
        --profile ${PROFILE}
else
    aws s3api create-bucket \
        --bucket ${BUCKET_NAME} \
        --region ${REGION} \
        --create-bucket-configuration LocationConstraint=${REGION} \
        --profile ${PROFILE}
fi

# Enable versioning
echo "Enabling versioning..."
aws s3api put-bucket-versioning \
    --bucket ${BUCKET_NAME} \
    --versioning-configuration Status=Enabled \
    --profile ${PROFILE}

# Enable encryption
echo "Enabling encryption (SSE-S3)..."
aws s3api put-bucket-encryption \
    --bucket ${BUCKET_NAME} \
    --server-side-encryption-configuration '{
        "Rules": [{
            "ApplyServerSideEncryptionByDefault": {
                "SSEAlgorithm": "AES256"
            },
            "BucketKeyEnabled": true
        }]
    }' \
    --profile ${PROFILE}

# Block public access
echo "Blocking public access..."
aws s3api put-public-access-block \
    --bucket ${BUCKET_NAME} \
    --public-access-block-configuration \
        "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true" \
    --profile ${PROFILE}

# Enable logging
echo "Enabling access logging..."
aws s3api put-bucket-logging \
    --bucket ${BUCKET_NAME} \
    --bucket-logging-status '{
        "LoggingEnabled": {
            "TargetBucket": "'${LOGS_BUCKET_NAME}'",
            "TargetPrefix": "access-logs/"
        }
    }' \
    --profile ${PROFILE}

# Add bucket tags
echo "Adding bucket tags..."
aws s3api put-bucket-tagging \
    --bucket ${BUCKET_NAME} \
    --tagging 'TagSet=[
        {Key=Environment,Value=production},
        {Key=Project,Value=media-analytics},
        {Key=Owner,Value=meagan},
        {Key=CostCenter,Value=data-engineering},
        {Key=DataClassification,Value=internal},
        {Key=Compliance,Value=GDPR},
        {Key=CapstoneProject,Value=true}
    ]' \
    --profile ${PROFILE}

# Create VPC endpoint policy
echo ""
echo "Creating VPC endpoint bucket policy..."
cat > /tmp/vpc-bucket-policy.json <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowSnowflakeVPCAccess",
            "Effect": "Allow",
            "Principal": "*",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": "arn:aws:s3:::${BUCKET_NAME}/*",
            "Condition": {
                "StringEquals": {
                    "aws:SourceVpc": "${SNOWFLAKE_VPC_ID}"
                }
            }
        },
        {
            "Sid": "AllowSnowflakeVPCList",
            "Effect": "Allow",
            "Principal": "*",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": "arn:aws:s3:::${BUCKET_NAME}",
            "Condition": {
                "StringEquals": {
                    "aws:SourceVpc": "${SNOWFLAKE_VPC_ID}"
                }
            }
        },
        {
            "Sid": "AllowCurrentAccountAccess",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::${ACCOUNT_ID}:root"
            },
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::${BUCKET_NAME}",
                "arn:aws:s3:::${BUCKET_NAME}/*"
            ]
        }
    ]
}
EOF

# Apply bucket policy
echo "Applying VPC endpoint bucket policy..."
aws s3api put-bucket-policy \
    --bucket ${BUCKET_NAME} \
    --policy file:///tmp/vpc-bucket-policy.json \
    --profile ${PROFILE}

# Create folder structure
echo ""
echo "Creating folder structure..."
folders=(
    "raw/viewership/"
    "raw/revenue/"
    "raw/content/"
    "raw/users/"
    "raw/subscriptions/"
    "stage/viewership/"
    "stage/revenue/"
    "stage/content/"
    "stage/users/"
    "stage/subscriptions/"
    "archive/"
    "errors/"
    "rejected/"
)

for folder in "${folders[@]}"; do
    echo "  Creating: ${folder}"
    aws s3api put-object \
        --bucket ${BUCKET_NAME} \
        --key "${folder}" \
        --profile ${PROFILE} > /dev/null
done

# Clean up temp file
rm /tmp/vpc-bucket-policy.json

echo ""
echo "================================================"
echo "✅ S3 Infrastructure Created Successfully!"
echo "================================================"
echo "Main Bucket: s3://${BUCKET_NAME}"
echo "Logs Bucket: s3://${LOGS_BUCKET_NAME}"
echo "VPC Restriction: ${SNOWFLAKE_VPC_ID}"
echo ""
echo "Security Features:"
echo "  ✅ VPC endpoint policy applied (Snowflake VPC only)"
echo "  ✅ Encryption enabled (SSE-S3)"
echo "  ✅ Versioning enabled"
echo "  ✅ Public access blocked"
echo "  ✅ Access logging enabled"
echo ""
echo "Next steps:"
echo "1. Run ./configure_lifecycle.sh to set lifecycle policies"
echo "2. Run ./setup_iam_roles_vpc.sh to create IAM role for Snowflake"
echo "3. Configure Snowflake storage integration"
echo "================================================"
