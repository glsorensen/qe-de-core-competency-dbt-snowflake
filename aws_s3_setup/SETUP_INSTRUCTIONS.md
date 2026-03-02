# AWS Setup for Meagan's Capstone Project

## Step 1: Configure AWS Credentials

You need to configure AWS CLI with your credentials before creating the S3 buckets.

### Option A: Using AWS SSO (Recommended for Organization Accounts)

If your organization uses AWS SSO:

```bash
aws configure sso
```

Follow the prompts:
- **SSO Start URL**: (Ask your AWS admin)
- **SSO Region**: us-east-1 (or your region)
- **Account**: 066396400174
- **Role**: Choose the role with S3 and IAM permissions

### Option B: Using Access Keys

If you have AWS access keys:

```bash
aws configure
```

Enter when prompted:
- **AWS Access Key ID**: [Your access key]
- **AWS Secret Access Key**: [Your secret key]
- **Default region name**: us-east-1
- **Default output format**: json

### Option C: Using Named Profiles

If you want to keep multiple AWS configurations:

```bash
aws configure --profile meagan-capstone
```

Then update the scripts to use this profile:
- Edit `create_s3_bucket.sh`
- Change `PROFILE="default"` to `PROFILE="meagan-capstone"`

## Step 2: Verify AWS Access

After configuring credentials, verify access:

```bash
# Test AWS credentials
aws sts get-caller-identity

# List existing S3 buckets
aws s3 ls

# Check your permissions
aws iam get-user
```

Expected output from `get-caller-identity`:
```json
{
    "UserId": "...",
    "Account": "066396400174",
    "Arn": "arn:aws:iam::066396400174:user/your-username"
}
```

## Step 3: Create S3 Buckets

Once credentials are configured:

```bash
cd /Users/meagan.ahmed/Catalyst/qe-de-core-competency-dbt-snowflake/aws_s3_setup
./create_s3_bucket.sh
```

This will create:
- **Main bucket**: `snowflake-media-analytics-meagan-066396400174`
- **Logs bucket**: `snowflake-media-analytics-meagan-066396400174-logs`
- Folder structure (raw/, stage/, archive/, errors/)
- Enable encryption, versioning, and access logging

## Step 4: Configure Lifecycle Policies

```bash
./configure_lifecycle.sh
```

This sets up automatic data lifecycle management:
- raw/ files: Transition to cheaper storage over time
- stage/ files: Delete after 7 days
- errors/ files: Delete after 30 days

## Troubleshooting

### Error: "Unable to locate credentials"
- Run `aws configure` and enter your credentials
- Or set up AWS SSO with `aws configure sso`

### Error: "Access Denied"
- Ensure your IAM user/role has permissions:
  - `s3:CreateBucket`
  - `s3:PutBucketVersioning`
  - `s3:PutEncryptionConfiguration`
  - `s3:PutLifecycleConfiguration`
  - `iam:CreateRole` (for IAM setup later)

### Error: "Bucket already exists"
- The bucket name must be globally unique
- If someone else created `snowflake-media-analytics-meagan-066396400174`, choose a different name
- Edit `create_s3_bucket.sh` and add more uniqueness (e.g., `meagan-ahmed-` or add a timestamp)

## Next Steps

After S3 setup is complete:
1. Generate sample data: `python generate_sample_data.py --upload-to-s3`
2. Configure Snowflake storage integration
3. Create IAM roles for Snowflake access
4. Set up data ingestion pipelines

## Resources Needed

**IAM Permissions Required**:
- S3: CreateBucket, PutBucket*, GetBucket*, ListBucket
- IAM: CreateRole, CreatePolicy, AttachRolePolicy (for later steps)
- SNS/SQS: CreateTopic, CreateQueue (for Snowpipe, optional)

**AWS CLI Documentation**:
- [Configuring AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html)
- [AWS SSO Configuration](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html)
