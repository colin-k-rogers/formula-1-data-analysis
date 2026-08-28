#!/usr/bin/env bash
# Shared config for setup.sh / run.sh. Edit these placeholders for your AWS
# account before running either script. This file is sourced, not executed
# directly.

export ACCOUNT_ID="123456789012"
export REGION="us-east-1"
export BUCKET_NAME="f1-radio-topics-lakehouse"
export REPO_NAME="f1-radio-topics"
export IMAGE_TAG="latest"
# Check `aws emr-serverless list-release-labels --region "$REGION"` for the
# current release — this may not be the latest by the time you read it.
export EMR_RELEASE="emr-7.5.0"
export CATALOG_NAME="radio"       # matches ICEBERG_CATALOG_NAME in job.py
export ICEBERG_NAMESPACE="raw"    # matches ICEBERG_NAMESPACE in job.py —
                                   # this becomes the Glue database name
export SEASON_YEAR="2025"

export MODEL_STORE_PATH="s3://${BUCKET_NAME}/models/bertopic/model.tar.gz"
export IMAGE="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}:${IMAGE_TAG}"
export ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/f1-radio-topics-emrs-role"
