#!/bin/bash
# Configure S3 lifecycle policies for data retention and cost optimization
# AWS Account: 066396400174

set -e

# Configuration
ACCOUNT_ID="066396400174"
BUCKET_NAME="snowflake-media-analytics-meagan-${ACCOUNT_ID}"
PROFILE="default"

echo "================================================"
echo "Configuring S3 Lifecycle Policies"
echo "================================================"
echo "Bucket: ${BUCKET_NAME}"
echo ""

# Create lifecycle configuration
cat > /tmp/lifecycle-config.json <<'EOF'
{
  "Rules": [
    {
      "Id": "transition-raw-to-ia",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "raw/"
      },
      "Transitions": [
        {
          "Days": 30,
          "StorageClass": "STANDARD_IA"
        },
        {
          "Days": 90,
          "StorageClass": "GLACIER_IR"
        },
        {
          "Days": 365,
          "StorageClass": "DEEP_ARCHIVE"
        }
      ]
    },
    {
      "Id": "cleanup-staging",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "stage/"
      },
      "Expiration": {
        "Days": 7
      }
    },
    {
      "Id": "archive-deep-storage",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "archive/"
      },
      "Transitions": [
        {
          "Days": 1,
          "StorageClass": "GLACIER_FLEXIBLE_RETRIEVAL"
        }
      ],
      "Expiration": {
        "Days": 2555
      }
    },
    {
      "Id": "cleanup-errors",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "errors/"
      },
      "Expiration": {
        "Days": 30
      }
    },
    {
      "Id": "cleanup-rejected",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "rejected/"
      },
      "Expiration": {
        "Days": 30
      }
    },
    {
      "Id": "cleanup-incomplete-multipart-uploads",
      "Status": "Enabled",
      "Filter": {},
      "AbortIncompleteMultipartUpload": {
        "DaysAfterInitiation": 7
      }
    }
  ]
}
EOF

echo "Applying lifecycle policy..."
aws s3api put-bucket-lifecycle-configuration \
    --bucket ${BUCKET_NAME} \
    --lifecycle-configuration file:///tmp/lifecycle-config.json \
    --profile ${PROFILE}

# Verify configuration
echo ""
echo "Verifying lifecycle configuration..."
aws s3api get-bucket-lifecycle-configuration \
    --bucket ${BUCKET_NAME} \
    --profile ${PROFILE}

# Clean up
rm /tmp/lifecycle-config.json

echo ""
echo "================================================"
echo "✅ Lifecycle Policies Configured Successfully!"
echo "================================================"
echo ""
echo "Policy Summary:"
echo "• raw/ files:"
echo "    - 0-30 days: STANDARD"
echo "    - 30-90 days: STANDARD_IA"
echo "    - 90-365 days: GLACIER_IR"
echo "    - 365+ days: DEEP_ARCHIVE"
echo "    - Never expire (archive indefinitely)"
echo ""
echo "• stage/ files:"
echo "    - Delete after 7 days"
echo ""
echo "• archive/ files:"
echo "    - Move to GLACIER after 1 day"
echo "    - Delete after 7 years (2555 days)"
echo ""
echo "• errors/ and rejected/ files:"
echo "    - Delete after 30 days"
echo ""
echo "• Incomplete multipart uploads:"
echo "    - Abort after 7 days"
echo ""
echo "================================================"
