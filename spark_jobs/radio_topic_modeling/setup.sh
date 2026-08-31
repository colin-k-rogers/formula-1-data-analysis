#!/usr/bin/env bash
# One-time AWS setup for the radio topic modeling Spark job — but safe to
# re-run: every resource is looked up by a fixed name/tag first, and only
# created if missing, so a partial failure or a "push a new image" rerun
# doesn't error out on "already exists" or pile up duplicate resources.
# (One exception: the IAM policy's content isn't diffed/updated on rerun —
# see step 3 — and the EMR Serverless application's config *is* updated in
# place, since rerunning after rebuilding the image is the common case.)
#
# Copy .env.example to .env and edit it first, then run this:
#   cp .env.example .env && ./setup.sh
# It writes the resource ids it finds/creates to state.sh (git-ignored),
# which run.sh reads.
set -euo pipefail
# Without this, the AWS CLI pipes any command's output through `less` when
# run in a terminal, which blocks waiting for you to press `q` on every
# single `aws` call below that doesn't already redirect its output.
export AWS_PAGER=""
cd "$(dirname "${BASH_SOURCE[0]}")"
if [ ! -f ./.env ]; then
    echo "Missing .env — copy the template and edit it first: cp .env.example .env" >&2
    exit 1
fi
set -a
source ./.env
set +a

IMAGE="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}:${IMAGE_TAG}"
ROLE_NAME="f1-radio-topics-emrs-role"
TAG_NAME="f1-radio-topics"

# 1. S3 bucket — holds the Iceberg warehouse data, EMR Serverless logs, and
#    the persisted BERTopic model (see run.sh's MODEL_STORE_PATH) — all
#    under one bucket so the IAM policy in step 3 already covers all of it.
if aws s3api head-bucket --bucket "$BUCKET_NAME" 2>/dev/null; then
    echo "S3 bucket exists: $BUCKET_NAME"
else
    aws s3 mb "s3://${BUCKET_NAME}" --region "$REGION"
fi

# 2. Networking: EMR Serverless without a VPC can only reach S3/Glue/
#    CloudWatch/STS/KMS/DynamoDB/Secrets Manager in-region — NOT the public
#    internet, which this job needs to reach the OpenF1 API. Attach it to a
#    VPC with a PUBLIC subnet (direct route to an internet gateway) instead
#    of a private one, so you get outbound internet without paying for a
#    NAT Gateway. Everything here is found-or-created by its Name tag.
VPC_ID=$(aws ec2 describe-vpcs --filters "Name=tag:Name,Values=${TAG_NAME}" \
    --query 'Vpcs[0].VpcId' --output text 2>/dev/null || true)
if [ -z "$VPC_ID" ] || [ "$VPC_ID" = "None" ]; then
    VPC_ID=$(aws ec2 create-vpc --cidr-block 10.0.0.0/16 --query 'Vpc.VpcId' --output text)
    aws ec2 create-tags --resources "$VPC_ID" --tags "Key=Name,Value=${TAG_NAME}"
    echo "Created VPC: $VPC_ID"
else
    echo "VPC exists: $VPC_ID"
fi

SUBNET_ID=$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=${VPC_ID}" "Name=tag:Name,Values=${TAG_NAME}" \
    --query 'Subnets[0].SubnetId' --output text 2>/dev/null || true)
if [ -z "$SUBNET_ID" ] || [ "$SUBNET_ID" = "None" ]; then
    SUBNET_ID=$(aws ec2 create-subnet --vpc-id "$VPC_ID" --cidr-block 10.0.1.0/24 --query 'Subnet.SubnetId' --output text)
    aws ec2 create-tags --resources "$SUBNET_ID" --tags "Key=Name,Value=${TAG_NAME}"
    echo "Created subnet: $SUBNET_ID"
else
    echo "Subnet exists: $SUBNET_ID"
fi
# Idempotent by nature (sets a fixed attribute) — safe to run every time
# regardless of whether the subnet above is new or pre-existing.
aws ec2 modify-subnet-attribute --subnet-id "$SUBNET_ID" --map-public-ip-on-launch

IGW_ID=$(aws ec2 describe-internet-gateways --filters "Name=attachment.vpc-id,Values=${VPC_ID}" \
    --query 'InternetGateways[0].InternetGatewayId' --output text 2>/dev/null || true)
if [ -z "$IGW_ID" ] || [ "$IGW_ID" = "None" ]; then
    IGW_ID=$(aws ec2 create-internet-gateway --query 'InternetGateway.InternetGatewayId' --output text)
    aws ec2 attach-internet-gateway --vpc-id "$VPC_ID" --internet-gateway-id "$IGW_ID"
    echo "Created + attached internet gateway: $IGW_ID"
else
    echo "Internet gateway exists: $IGW_ID"
fi

RT_ID=$(aws ec2 describe-route-tables --filters "Name=vpc-id,Values=${VPC_ID}" "Name=tag:Name,Values=${TAG_NAME}" \
    --query 'RouteTables[0].RouteTableId' --output text 2>/dev/null || true)
if [ -z "$RT_ID" ] || [ "$RT_ID" = "None" ]; then
    RT_ID=$(aws ec2 create-route-table --vpc-id "$VPC_ID" --query 'RouteTable.RouteTableId' --output text)
    aws ec2 create-tags --resources "$RT_ID" --tags "Key=Name,Value=${TAG_NAME}"
    aws ec2 create-route --route-table-id "$RT_ID" --destination-cidr-block 0.0.0.0/0 --gateway-id "$IGW_ID"
    aws ec2 associate-route-table --route-table-id "$RT_ID" --subnet-id "$SUBNET_ID"
    echo "Created route table: $RT_ID"
else
    echo "Route table exists: $RT_ID"
fi

SG_ID=$(aws ec2 describe-security-groups --filters "Name=vpc-id,Values=${VPC_ID}" "Name=group-name,Values=${TAG_NAME}-emr" \
    --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || true)
if [ -z "$SG_ID" ] || [ "$SG_ID" = "None" ]; then
    SG_ID=$(aws ec2 create-security-group --group-name "${TAG_NAME}-emr" --description "EMR Serverless egress for radio topic modeling" --vpc-id "$VPC_ID" --query 'GroupId' --output text)
    echo "Created security group: $SG_ID"
    # New security groups already default to "allow all outbound" (just no
    # inbound rules, which this job doesn't need), so no egress rule to add.
else
    echo "Security group exists: $SG_ID"
fi

# 3. IAM job runtime role — what the *job* is allowed to touch (S3, Glue).
# get-role/get-policy existing is treated as "good enough" here — this
# doesn't diff and update their content on rerun, so if you change the
# trust or access policy documents below, update the existing role/policy
# by hand (or delete them first) rather than relying on rerunning this.
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
if aws iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
    echo "IAM role exists: $ROLE_NAME"
else
    aws iam create-role --role-name "$ROLE_NAME" \
        --assume-role-policy-document file:///tmp/emrs-trust-policy.json
    echo "Created IAM role: $ROLE_NAME"
fi

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
POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/f1-radio-topics-emrs-policy"
if aws iam get-policy --policy-arn "$POLICY_ARN" >/dev/null 2>&1; then
    echo "IAM policy exists: $POLICY_ARN"
else
    aws iam create-policy --policy-name f1-radio-topics-emrs-policy \
        --policy-document file:///tmp/emrs-access-policy.json
    echo "Created IAM policy: $POLICY_ARN"
fi
# Attaching an already-attached policy is a no-op — safe every time.
aws iam attach-role-policy --role-name "$ROLE_NAME" --policy-arn "$POLICY_ARN"

# 4. ECR repo + build/push the custom container image (needed because
#    faster-whisper/BERTopic/torch are too heavy to pip-install on every
#    job run — see ./Dockerfile).
if aws ecr describe-repositories --repository-names "$REPO_NAME" --region "$REGION" >/dev/null 2>&1; then
    echo "ECR repo exists: $REPO_NAME"
else
    aws ecr create-repository --repository-name "$REPO_NAME" --region "$REGION"
    echo "Created ECR repo: $REPO_NAME"
fi

# ECR repository (resource) policies reject a "Resource" element outright
# ("InvalidParameterException: Invalid repository policy provided") — it's
# implicit from which repository the policy is attached to, unlike an IAM
# identity policy where you'd need one.
cat > /tmp/emrs-ecr-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "EmrServerlessCustomImageSupport",
    "Effect": "Allow",
    "Principal": {"Service": "emr-serverless.amazonaws.com"},
    "Action": ["ecr:BatchGetImage", "ecr:DescribeImages", "ecr:GetDownloadUrlForLayer"],
    "Condition": {"ArnLike": {"aws:SourceArn": "arn:aws:emr-serverless:${REGION}:${ACCOUNT_ID}:/applications/*"}}
  }]
}
EOF
# set-repository-policy always overwrites — safe every time.
aws ecr set-repository-policy --repository-name "$REPO_NAME" --policy-text file:///tmp/emrs-ecr-policy.json

# Rebuilding/pushing the same tag just overwrites it — safe every time.
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"
docker build -t "$IMAGE" .
docker push "$IMAGE"

# 5. EMR Serverless application, wired to the VPC's public subnet and the
#    custom image. Unlike the resources above, an existing application is
#    updated in place (not just reused as-is) — rerunning this after
#    pushing a new image is the point.
APP_ID=$(aws emr-serverless list-applications \
    --query "applications[?name=='${TAG_NAME}'] | [0].id" --output text 2>/dev/null || true)
if [ -z "$APP_ID" ] || [ "$APP_ID" = "None" ]; then
    APP_ID=$(aws emr-serverless create-application \
        --name "$TAG_NAME" \
        --release-label "$EMR_RELEASE" \
        --type SPARK \
        --image-configuration "{\"imageUri\": \"${IMAGE}\"}" \
        --network-configuration "{\"subnetIds\": [\"${SUBNET_ID}\"], \"securityGroupIds\": [\"${SG_ID}\"]}" \
        --query 'applicationId' --output text)
    echo "Created EMR Serverless application: $APP_ID"
else
    aws emr-serverless update-application \
        --application-id "$APP_ID" \
        --image-configuration "{\"imageUri\": \"${IMAGE}\"}" \
        --network-configuration "{\"subnetIds\": [\"${SUBNET_ID}\"], \"securityGroupIds\": [\"${SG_ID}\"]}" \
        >/dev/null
    echo "Updated EMR Serverless application: $APP_ID"
fi

cat > ./state.sh <<EOF
# Generated by setup.sh — resource ids run.sh needs. Not committed (see
# .gitignore); re-run setup.sh any time to refresh these.
export VPC_ID="${VPC_ID}"
export SUBNET_ID="${SUBNET_ID}"
export SG_ID="${SG_ID}"
export APP_ID="${APP_ID}"
EOF
echo "Wrote state.sh — run ./run.sh next."
