#!/usr/bin/env bash
# Creates the IAM user the f1-radio-topics-pipeline Flight submits EMR
# Serverless job runs as -- see the README section on that Flight for why
# it's separate from setup_iam_user.sh's read-only reader. MotherDuck runs
# outside AWS, so it needs a static key pair, not an assumable role.
#
# Reads ../spark_jobs/radio_topic_modeling/.env and state.sh, so run that
# job's setup.sh first. Run this once:
#   ./setup_flight_submitter_iam_user.sh
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
source "$STATE_FILE"   # APP_ID

USER_NAME="f1-radio-topics-flight-submitter"
POLICY_NAME="f1-radio-topics-flight-submitter-policy"
POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/${POLICY_NAME}"
APP_ARN="arn:aws:emr-serverless:${REGION}:${ACCOUNT_ID}:/applications/${APP_ID}"
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/f1-radio-topics-emrs-role"

if aws iam get-user --user-name "$USER_NAME" >/dev/null 2>&1; then
    echo "IAM user exists: $USER_NAME"
else
    aws iam create-user --user-name "$USER_NAME"
    echo "Created IAM user: $USER_NAME"
fi

# GetApplication is what start_job_run itself calls before submitting.
# PassRole is what lets a job run assume the runtime role, so it's kept
# tight: exactly that role, handed to exactly EMR Serverless.
cat > /tmp/flight-submitter-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SubmitAndWatchJobRuns",
      "Effect": "Allow",
      "Action": [
        "emr-serverless:GetApplication",
        "emr-serverless:StartJobRun",
        "emr-serverless:GetJobRun",
        "emr-serverless:ListJobRuns"
      ],
      "Resource": ["${APP_ARN}", "${APP_ARN}/jobruns/*"]
    },
    {
      "Sid": "PassJobRuntimeRoleToEmrServerlessOnly",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "${ROLE_ARN}",
      "Condition": {
        "StringEquals": {"iam:PassedToService": "emr-serverless.amazonaws.com"}
      }
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
        --policy-document file:///tmp/flight-submitter-policy.json --set-as-default >/dev/null
    echo "Updated IAM policy: $POLICY_ARN"
else
    aws iam create-policy --policy-name "$POLICY_NAME" \
        --policy-document file:///tmp/flight-submitter-policy.json
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
    echo "    (after updating the aws_emr Flight secret in MotherDuck)"
    exit 0
fi

read -r ACCESS_KEY_ID SECRET_ACCESS_KEY <<<"$(aws iam create-access-key --user-name "$USER_NAME" \
    --query 'AccessKey.[AccessKeyId,SecretAccessKey]' --output text)"

echo ""
echo "Created an access key for $USER_NAME -- the secret below is shown only this once:"
echo "  AWS_ACCESS_KEY_ID:     $ACCESS_KEY_ID"
echo "  AWS_SECRET_ACCESS_KEY: $SECRET_ACCESS_KEY"
echo ""
echo "Put them in a MotherDuck Flight secret named 'aws_emr' (the name"
echo "flights/radio_topics_pipeline/main.py reads) -- this link prefills the"
echo "form with the name and parameter names, so only you ever see the values:"
echo "  https://app.motherduck.com/settings/secrets?action=create&type=flights&name=aws_emr&params=AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY"
