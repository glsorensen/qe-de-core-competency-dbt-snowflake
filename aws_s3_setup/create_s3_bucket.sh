#!/bin/bash
# Create S3 bucket for Snowflake media analytics pipeline
# AWS Account: 066396400174

set -e  # Exit on error

# Configuration
ACCOUNT_ID="066396400174"
BUCKET_NAME="snowflake-media-analytics-meagan-${ACCOUNT_ID}"
LOGS_BUCKET_NAME="${BUCKET_NAME}-logs"
REGION="us-east-1"  # Update to your preferred region
PROFILE="default"   # Update if using named AWS profile

echo "================================================"
echo "Creating S3 Infrastructure for Snowflake Pipeline"
echo "================================================"
echo "Account ID: ${ACCOUNT_ID}"
echo "Region: ${REGION}"
echo "Bucket: ${BUCKET_NAME}"
echo ""

# Check AWS CLI is configured
if ! command -v aws &> /dev/null; then
    echo "Error: AWS CLI is not installed"
    exit 1
fi

# Verify AWS credentials
echo "Verifying AWS credentials..."
aws sts get-caller-identity --profile ${PROFILE}

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
        {Key=CostCenter,Value=data-engineering},
        {Key=DataClassification,Value=internal},
        {Key=Compliance,Value=GDPR},
        {Key=Owner,Value=data-platform-team},
        {Key=ManagedBy,Value=terraform}
    ]' \
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

echo ""
echo "================================================"
echo "✅ S3 Infrastructure Created Successfully!"
echo "================================================"
echo "Main Bucket: s3://${BUCKET_NAME}"
echo "Logs Bucket: s3://${LOGS_BUCKET_NAME}"
echo ""
echo "Next steps:"
echo "1. Run setup_iam_roles.sh to create IAM roles"
echo "2. Run configure_lifecycle.sh to set lifecycle policies"
echo "3. Configure Snowflake external stages"
echo "================================================"
