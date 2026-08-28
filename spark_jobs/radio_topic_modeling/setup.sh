#!/usr/bin/env bash
# One-time AWS setup (from scratch) for the radio topic modeling Spark job.
# Edit config.sh first, then run this once:
#   ./setup.sh
# It writes the resource ids it creates to state.sh (git-ignored), which
# run.sh reads — re-run this if you ever need to recreate them from scratch.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source ./config.sh

# 1. S3 bucket — holds the Iceberg warehouse data, EMR Serverless logs, and
#    the persisted BERTopic model (MODEL_STORE_PATH) — all under one bucket
#    so the IAM policy in step 3 already covers all of it.
aws s3 mb "s3://${BUCKET_NAME}" --region "$REGION"

# 2. Networking: EMR Serverless without a VPC can only reach S3/Glue/
#    CloudWatch/STS/KMS/DynamoDB/Secrets Manager in-region — NOT the public
#    internet, which this job needs to reach the OpenF1 API. Attach it to a
#    VPC with a PUBLIC subnet (direct route to an internet gateway) instead
#    of a private one, so you get outbound internet without paying for a
#    NAT Gateway. If you don't already have a VPC to reuse:
VPC_ID=$(aws ec2 create-vpc --cidr-block 10.0.0.0/16 --query 'Vpc.VpcId' --output text)
SUBNET_ID=$(aws ec2 create-subnet --vpc-id "$VPC_ID" --cidr-block 10.0.1.0/24 --query 'Subnet.SubnetId' --output text)
IGW_ID=$(aws ec2 create-internet-gateway --query 'InternetGateway.InternetGatewayId' --output text)
aws ec2 attach-internet-gateway --vpc-id "$VPC_ID" --internet-gateway-id "$IGW_ID"
RT_ID=$(aws ec2 create-route-table --vpc-id "$VPC_ID" --query 'RouteTable.RouteTableId' --output text)
aws ec2 create-route --route-table-id "$RT_ID" --destination-cidr-block 0.0.0.0/0 --gateway-id "$IGW_ID"
aws ec2 associate-route-table --route-table-id "$RT_ID" --subnet-id "$SUBNET_ID"
aws ec2 modify-subnet-attribute --subnet-id "$SUBNET_ID" --map-public-ip-on-launch
SG_ID=$(aws ec2 create-security-group --group-name f1-radio-topics-emr --description "EMR Serverless egress for radio topic modeling" --vpc-id "$VPC_ID" --query 'GroupId' --output text)
# New security groups already default to "allow all outbound" (just no
# inbound rules, which this job doesn't need), so no egress rule to add.

# 3. IAM job runtime role — what the *job* is allowed to touch (S3, Glue).
cat > /tmp/emrs-trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "emr-serverless.amazonaws.com"},
    "Action": "sts:AssumeRole",
    "Condition": {"StringEquals": {"aws:SourceAccount": "${ACCOUNT_ID}"}}
  }]
}
EOF
aws iam create-role --role-name f1-radio-topics-emrs-role \
    --assume-role-policy-document file:///tmp/emrs-trust-policy.json

cat > /tmp/emrs-access-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3Warehouse",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::${BUCKET_NAME}", "arn:aws:s3:::${BUCKET_NAME}/*"]
    },
    {
      "Sid": "GlueCatalog",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase", "glue:CreateDatabase", "glue:GetDatabases",
        "glue:CreateTable", "glue:GetTable", "glue:GetTables",
        "glue:UpdateTable", "glue:DeleteTable",
        "glue:GetPartition", "glue:GetPartitions",
        "glue:CreatePartition", "glue:BatchCreatePartition"
      ],
      "Resource": "*"
    }
  ]
}
EOF
aws iam create-policy --policy-name f1-radio-topics-emrs-policy \
    --policy-document file:///tmp/emrs-access-policy.json
aws iam attach-role-policy --role-name f1-radio-topics-emrs-role \
    --policy-arn "arn:aws:iam::${ACCOUNT_ID}:policy/f1-radio-topics-emrs-policy"

# 4. ECR repo + build/push the custom container image (needed because
#    faster-whisper/BERTopic/torch are too heavy to pip-install on every
#    job run — see ./Dockerfile).
aws ecr create-repository --repository-name "$REPO_NAME" --region "$REGION"

cat > /tmp/emrs-ecr-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "EmrServerlessCustomImageSupport",
    "Effect": "Allow",
    "Principal": {"Service": "emr-serverless.amazonaws.com"},
    "Action": ["ecr:BatchGetImage", "ecr:DescribeImages", "ecr:GetDownloadUrlForLayer"],
    "Resource": "arn:aws:ecr:${REGION}:${ACCOUNT_ID}:repository/${REPO_NAME}",
    "Condition": {"ArnLike": {"aws:SourceArn": "arn:aws:emr-serverless:${REGION}:${ACCOUNT_ID}:/applications/*"}}
  }]
}
EOF
aws ecr set-repository-policy --repository-name "$REPO_NAME" --policy-text file:///tmp/emrs-ecr-policy.json

aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"
docker build -t "$IMAGE" .
docker push "$IMAGE"

# 5. EMR Serverless application, wired to the VPC's public subnet and the
#    custom image.
APP_ID=$(aws emr-serverless create-application \
    --name f1-radio-topics \
    --release-label "$EMR_RELEASE" \
    --type SPARK \
    --image-configuration "{\"imageUri\": \"${IMAGE}\"}" \
    --network-configuration "{\"subnetIds\": [\"${SUBNET_ID}\"], \"securityGroupIds\": [\"${SG_ID}\"]}" \
    --query 'applicationId' --output text)
echo "EMR Serverless application: $APP_ID"

cat > ./state.sh <<EOF
# Generated by setup.sh — resource ids run.sh needs. Not committed (see
# .gitignore); re-run setup.sh if you ever need to recreate them.
export VPC_ID="${VPC_ID}"
export SUBNET_ID="${SUBNET_ID}"
export SG_ID="${SG_ID}"
export APP_ID="${APP_ID}"
EOF
echo "Wrote state.sh — run ./run.sh next."
