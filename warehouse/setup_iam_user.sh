#!/usr/bin/env bash
# Creates a read-only IAM user for MotherDuck to authenticate as when
# attaching the Glue Data Catalog (see setup_radio_lakehouse.sql).
# MotherDuck is an external SaaS service, not running inside AWS, so it
# needs a static AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY pair rather than
# an assumable IAM role (unlike the EMR Serverless job's own execution
# role, which AWS-hosted compute can assume).
#
# Reuses ../spark_jobs/radio_topic_modeling/.env for ACCOUNT_ID/BUCKET_NAME
# -- run that Flight's setup.sh first if you haven't.
#
# Safe to re-run: the user/policy are found-or-created by name, same as
# spark_jobs/radio_topic_modeling/setup.sh. The access key is the one
# exception -- IAM only ever shows you its secret at creation time, so
# this skips creating a second one if the user already has one (also
# because an IAM user can hold at most 2 access keys at a time).
#
# Run this once:
#   ./setup_iam_user.sh
set -euo pipefail
export AWS_PAGER=""
cd "$(dirname "${BASH_SOURCE[0]}")"

ENV_FILE="../spark_jobs/radio_topic_modeling/.env"
if [ ! -f "$ENV_FILE" ]; then
    echo "Missing $ENV_FILE -- run spark_jobs/radio_topic_modeling/setup.sh first (or at least copy its .env.example to .env) so ACCOUNT_ID/BUCKET_NAME are set." >&2
    exit 1
fi
set -a
source "$ENV_FILE"
set +a

USER_NAME="f1-radio-topics-motherduck-reader"
POLICY_NAME="f1-radio-topics-motherduck-reader-policy"
POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/${POLICY_NAME}"

if aws iam get-user --user-name "$USER_NAME" >/dev/null 2>&1; then
    echo "IAM user exists: $USER_NAME"
else
    aws iam create-user --user-name "$USER_NAME"
    echo "Created IAM user: $USER_NAME"
fi

# Read-only and narrower than the EMR job's own execution role (no
# Create/Update/Delete/BatchCreate) -- MotherDuck only ever reads through
# this, matching setup_radio_lakehouse.sql's READ_ONLY true.
cat > /tmp/motherduck-reader-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "GlueReadOnly",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase", "glue:GetDatabases",
        "glue:GetTable", "glue:GetTables",
        "glue:GetPartition", "glue:GetPartitions"
      ],
      "Resource": "*"
    },
    {
      "Sid": "S3ReadOnly",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::${BUCKET_NAME}", "arn:aws:s3:::${BUCKET_NAME}/*"]
    }
  ]
}
EOF
if aws iam get-policy --policy-arn "$POLICY_ARN" >/dev/null 2>&1; then
    echo "IAM policy exists: $POLICY_ARN"
else
    aws iam create-policy --policy-name "$POLICY_NAME" \
        --policy-document file:///tmp/motherduck-reader-policy.json
    echo "Created IAM policy: $POLICY_ARN"
fi
# Attaching an already-attached policy is a no-op -- safe every time.
aws iam attach-user-policy --user-name "$USER_NAME" --policy-arn "$POLICY_ARN"

EXISTING_KEY_ID=$(aws iam list-access-keys --user-name "$USER_NAME" \
    --query 'AccessKeyMetadata[0].AccessKeyId' --output text)
if [ -n "$EXISTING_KEY_ID" ] && [ "$EXISTING_KEY_ID" != "None" ]; then
    echo "This user already has an access key ($EXISTING_KEY_ID) -- IAM only shows"
    echo "the secret once, at creation, so it can't be printed again here. Either:"
    echo "  - reuse the value you saved when it was created, or"
    echo "  - rotate it: aws iam create-access-key --user-name $USER_NAME"
    echo "    then aws iam delete-access-key --user-name $USER_NAME --access-key-id $EXISTING_KEY_ID"
    echo "    (after updating the secret in setup_radio_lakehouse.sql / MotherDuck)"
    exit 0
fi

read -r ACCESS_KEY_ID SECRET_ACCESS_KEY <<<"$(aws iam create-access-key --user-name "$USER_NAME" \
    --query 'AccessKey.[AccessKeyId,SecretAccessKey]' --output text)"

echo ""
echo "Created an access key for $USER_NAME -- the secret below is shown only this once:"
echo "  KEY_ID: $ACCESS_KEY_ID"
echo "  SECRET: $SECRET_ACCESS_KEY"
echo ""
echo "Paste these into setup_radio_lakehouse.sql's CREATE SECRET statement"
echo "(KEY_ID / SECRET / REGION=${REGION}), and WAREHOUSE=${ACCOUNT_ID} on the"
echo "CREATE DATABASE below it, then run that file against your MotherDuck workspace."
