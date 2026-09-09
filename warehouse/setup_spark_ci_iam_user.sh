#!/usr/bin/env bash
# Creates the IAM user GitHub Actions authenticates as to build/push a
# PR-staging image and point the staging EMR Serverless application at it
# (.github/workflows/deploy_spark_staging.yaml,
# .github/workflows/cleanup_spark_staging.yaml). GitHub Actions runs
# outside AWS, so it needs a static key pair, not an assumable role -- same
# reasoning as setup_flight_submitter_iam_user.sh and setup_iam_user.sh.
#
# Reads ../spark_jobs/radio_topic_modeling/.env and state.sh, so run that
# job's setup.sh AND setup_staging.sh first. Run this once:
#   ./setup_spark_ci_iam_user.sh
set -euo pipefail
export AWS_PAGER=""
cd "$(dirname "${BASH_SOURCE[0]}")"

ENV_FILE="../spark_jobs/radio_topic_modeling/.env"
STATE_FILE="../spark_jobs/radio_topic_modeling/state.sh"
for required in "$ENV_FILE" "$STATE_FILE"; do
    if [ ! -f "$required" ]; then
        echo "Missing $required -- run spark_jobs/radio_topic_modeling/setup.sh first." >&2
        exit 1
    fi
done
set -a
source "$ENV_FILE"
set +a
source "$STATE_FILE"   # STAGING_APP_ID (written by setup_staging.sh)

if [ -z "${STAGING_APP_ID:-}" ]; then
    echo "state.sh has no STAGING_APP_ID -- run spark_jobs/radio_topic_modeling/setup_staging.sh first." >&2
    exit 1
fi

USER_NAME="f1-radio-topics-ci"
POLICY_NAME="f1-radio-topics-ci-policy"
POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/${POLICY_NAME}"
REPO_ARN="arn:aws:ecr:${REGION}:${ACCOUNT_ID}:repository/${REPO_NAME}"
STAGING_APP_ARN="arn:aws:emr-serverless:${REGION}:${ACCOUNT_ID}:/applications/${STAGING_APP_ID}"

if aws iam get-user --user-name "$USER_NAME" >/dev/null 2>&1; then
    echo "IAM user exists: $USER_NAME"
else
    aws iam create-user --user-name "$USER_NAME"
    echo "Created IAM user: $USER_NAME"
fi

# GetAuthorizationToken is account-wide by design (ECR has no
# resource-level permissions for it) -- everything else is scoped to just
# this one repo and this one (staging, never production) application.
cat > /tmp/spark-ci-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "EcrAuth",
      "Effect": "Allow",
      "Action": "ecr:GetAuthorizationToken",
      "Resource": "*"
    },
    {
      "Sid": "EcrPushAndCleanup",
      "Effect": "Allow",
      "Action": [
        "ecr:BatchCheckLayerAvailability",
        "ecr:InitiateLayerUpload",
        "ecr:UploadLayerPart",
        "ecr:CompleteLayerUpload",
        "ecr:PutImage",
        "ecr:BatchGetImage",
        "ecr:GetDownloadUrlForLayer",
        "ecr:DescribeImages",
        "ecr:ListImages",
        "ecr:BatchDeleteImage"
      ],
      "Resource": "${REPO_ARN}"
    },
    {
      "Sid": "UpdateStagingApplicationOnly",
      "Effect": "Allow",
      "Action": [
        "emr-serverless:GetApplication",
        "emr-serverless:UpdateApplication"
      ],
      "Resource": "${STAGING_APP_ARN}"
    }
  ]
}
EOF
if aws iam get-policy --policy-arn "$POLICY_ARN" >/dev/null 2>&1; then
    # As in setup_iam_user.sh: policies can't be edited in place, cap of 5.
    VERSION_COUNT=$(aws iam list-policy-versions --policy-arn "$POLICY_ARN" --query 'length(Versions)' --output text)
    if [ "$VERSION_COUNT" -ge 5 ]; then
        OLDEST_VERSION=$(aws iam list-policy-versions --policy-arn "$POLICY_ARN" \
            --query 'sort_by(Versions[?IsDefaultVersion==`false`], &CreateDate)[0].VersionId' --output text)
        aws iam delete-policy-version --policy-arn "$POLICY_ARN" --version-id "$OLDEST_VERSION"
    fi
    aws iam create-policy-version --policy-arn "$POLICY_ARN" \
        --policy-document file:///tmp/spark-ci-policy.json --set-as-default >/dev/null
    echo "Updated IAM policy: $POLICY_ARN"
else
    aws iam create-policy --policy-name "$POLICY_NAME" \
        --policy-document file:///tmp/spark-ci-policy.json
    echo "Created IAM policy: $POLICY_ARN"
fi
aws iam attach-user-policy --user-name "$USER_NAME" --policy-arn "$POLICY_ARN"

EXISTING_KEY_ID=$(aws iam list-access-keys --user-name "$USER_NAME" \
    --query 'AccessKeyMetadata[0].AccessKeyId' --output text)
if [ -n "$EXISTING_KEY_ID" ] && [ "$EXISTING_KEY_ID" != "None" ]; then
    echo "This user already has an access key ($EXISTING_KEY_ID) -- IAM only shows"
    echo "the secret once, at creation, so it can't be printed again here. Either:"
    echo "  - reuse the value you saved when it was created, or"
    echo "  - rotate it: aws iam create-access-key --user-name $USER_NAME"
    echo "    then aws iam delete-access-key --user-name $USER_NAME --access-key-id $EXISTING_KEY_ID"
    echo "    (after updating the AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY GitHub secrets)"
    exit 0
fi

read -r ACCESS_KEY_ID SECRET_ACCESS_KEY <<<"$(aws iam create-access-key --user-name "$USER_NAME" \
    --query 'AccessKey.[AccessKeyId,SecretAccessKey]' --output text)"

echo ""
echo "Created an access key for $USER_NAME -- the secret below is shown only this once:"
echo "  AWS_ACCESS_KEY_ID:     $ACCESS_KEY_ID"
echo "  AWS_SECRET_ACCESS_KEY: $SECRET_ACCESS_KEY"
echo ""
echo "Set these as GitHub Actions repository secrets, and the ids below as"
echo "repository variables (Settings > Secrets and variables > Actions):"
echo "  gh secret set AWS_ACCESS_KEY_ID --body '$ACCESS_KEY_ID'"
echo "  gh secret set AWS_SECRET_ACCESS_KEY --body '$SECRET_ACCESS_KEY'"
echo "  gh variable set AWS_REGION --body '$REGION'"
echo "  gh variable set ECR_REPO_NAME --body '$REPO_NAME'"
echo "  gh variable set EMR_STAGING_APPLICATION_ID --body '$STAGING_APP_ID'"
