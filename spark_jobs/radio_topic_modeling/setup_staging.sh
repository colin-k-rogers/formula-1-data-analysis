#!/usr/bin/env bash
# One-time setup for the shared PR-staging EMR Serverless application —
# see README.md's "PR staging" section for the full design and tradeoffs.
# Reuses everything ./setup.sh already created for production (VPC/IAM
# role/ECR repo); only creates the one additional application.
#
# Run ./setup.sh first. Then:
#   ./setup_staging.sh
#
# Safe to re-run, same as setup.sh: found by name first, only created if
# missing. Writes STAGING_APP_ID into state.sh alongside setup.sh's own
# entries (APP_ID stays production's).
set -euo pipefail
export AWS_PAGER=""
cd "$(dirname "${BASH_SOURCE[0]}")"
if [ ! -f ./.env ]; then
    echo "Missing .env — copy the template and edit it first: cp .env.example .env" >&2
    exit 1
fi
if [ ! -f ./state.sh ]; then
    echo "Missing state.sh — run ./setup.sh first (creates the VPC/subnet/security group/IAM role/ECR image this reuses)." >&2
    exit 1
fi
set -a
source ./.env
set +a
source ./state.sh   # VPC_ID, SUBNET_ID, SG_ID, APP_ID (production)

if [ -z "${VPC_ID:-}" ] || [ -z "${SUBNET_ID:-}" ] || [ -z "${SG_ID:-}" ]; then
    echo "state.sh is missing VPC_ID/SUBNET_ID/SG_ID — run ./setup.sh first." >&2
    exit 1
fi

IMAGE="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}:${IMAGE_TAG}"
case "$ARCHITECTURE" in
    arm64) EMR_ARCHITECTURE="ARM64" ;;
    x86_64) EMR_ARCHITECTURE="X86_64" ;;
    *)
        echo "ARCHITECTURE must be 'arm64' or 'x86_64' in .env (got: '${ARCHITECTURE}')" >&2
        exit 1
        ;;
esac

STAGING_APP_NAME="f1-radio-topics-staging"
STAGING_APP_ID=$(aws emr-serverless list-applications \
    --query "applications[?name=='${STAGING_APP_NAME}'] | [0].id" --output text 2>/dev/null || true)
if [ -z "$STAGING_APP_ID" ] || [ "$STAGING_APP_ID" = "None" ]; then
    # Seeded with production's current image just so the application is
    # immediately submittable — the deploy_spark_staging.yaml workflow
    # overwrites this with each PR's own image via update-application.
    STAGING_APP_ID=$(aws emr-serverless create-application \
        --name "$STAGING_APP_NAME" \
        --release-label "$EMR_RELEASE" \
        --type SPARK \
        --architecture "$EMR_ARCHITECTURE" \
        --image-configuration "{\"imageUri\": \"${IMAGE}\"}" \
        --network-configuration "{\"subnetIds\": [\"${SUBNET_ID}\"], \"securityGroupIds\": [\"${SG_ID}\"]}" \
        --query 'applicationId' --output text)
    echo "Created staging EMR Serverless application: $STAGING_APP_ID ($EMR_ARCHITECTURE)"
else
    echo "Staging EMR Serverless application exists: $STAGING_APP_ID"
fi

# Appends rather than rewrites state.sh so a re-run doesn't clobber
# whatever ./setup.sh most recently wrote for production.
if grep -q '^export STAGING_APP_ID=' ./state.sh 2>/dev/null; then
    # Portable in-place edit across BSD/GNU sed without a temp-file dance.
    sed -i.bak "s/^export STAGING_APP_ID=.*/export STAGING_APP_ID=\"${STAGING_APP_ID}\"/" ./state.sh
    rm -f ./state.sh.bak
else
    {
        echo "export STAGING_APP_ID=\"${STAGING_APP_ID}\""
    } >> ./state.sh
fi
echo "Wrote STAGING_APP_ID to state.sh."
echo ""
echo "GitHub Actions needs this id too (repo variable EMR_STAGING_APPLICATION_ID)"
echo "and a CI IAM user to push images and update it — see"
echo "../../warehouse/setup_spark_ci_iam_user.sh and README.md's \"PR staging\" section."
