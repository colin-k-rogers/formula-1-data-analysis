#!/usr/bin/env bash
# Submits an EMR Serverless job run:
#   ./run.sh
# Re-run any time you want to pick up a newly-run race weekend's radio —
# it's incremental (see README.md). Set FORCE_REFIT=true or
# REPROCESS_SESSIONS="<comma-separated session_key list>" in the
# environment before calling this script to opt into those, e.g.:
#   FORCE_REFIT=true ./run.sh
set -euo pipefail
# Without this, the AWS CLI pipes any command's output through `less` when
# run in a terminal, which blocks waiting for you to press `q`.
export AWS_PAGER=""
cd "$(dirname "${BASH_SOURCE[0]}")"
if [ ! -f ./.env ]; then
    echo "Missing .env — copy the template and edit it first: cp .env.example .env" >&2
    exit 1
fi
if [ ! -f ./state.sh ]; then
    echo "Missing state.sh — run ./setup.sh first." >&2
    exit 1
fi
# Preserve a SEASON_YEAR passed on the command line (e.g.
# `SEASON_YEAR=2024 ./run.sh`) -- .env also defines SEASON_YEAR as its
# default, and sourcing it below would otherwise silently clobber the
# caller's override.
SEASON_YEAR_OVERRIDE="${SEASON_YEAR:-}"
set -a
source ./.env
set +a
if [ -n "$SEASON_YEAR_OVERRIDE" ]; then
    SEASON_YEAR="$SEASON_YEAR_OVERRIDE"
fi
source ./state.sh   # written by setup.sh: APP_ID, VPC_ID, SUBNET_ID, SG_ID

MODEL_STORE_PATH="s3://${BUCKET_NAME}/models/bertopic/model.tar.gz"
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/f1-radio-topics-emrs-role"
# Falls back to the Dockerfile/setup.sh default if .env predates this
# variable -- must match whatever size the image was actually built with
# (see setup.sh's --build-arg), since this only controls what the running
# job requests, not what's pre-baked.
WHISPER_MODEL_SIZE="${WHISPER_MODEL_SIZE:-medium}"

EXTRA_CONF=""
if [ -n "${FORCE_REFIT:-}" ]; then
    EXTRA_CONF+=" --conf spark.emr-serverless.driverEnv.FORCE_REFIT=${FORCE_REFIT}"
fi
if [ -n "${REPROCESS_SESSIONS:-}" ]; then
    EXTRA_CONF+=" --conf spark.emr-serverless.driverEnv.REPROCESS_SESSIONS=${REPROCESS_SESSIONS}"
fi

JOB_RUN_ID=$(aws emr-serverless start-job-run \
    --application-id "$APP_ID" \
    --execution-role-arn "$ROLE_ARN" \
    --job-driver "{
      \"sparkSubmit\": {
        \"entryPoint\": \"local:///opt/radio_topic_modeling/job.py\",
        \"sparkSubmitParameters\": \"--conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=/opt/venv/bin/python --conf spark.executorEnv.PYSPARK_PYTHON=/opt/venv/bin/python --conf spark.emr-serverless.driverEnv.SEASON_YEAR=${SEASON_YEAR} --conf spark.emr-serverless.driverEnv.MODEL_STORE_PATH=${MODEL_STORE_PATH} --conf spark.executorEnv.WHISPER_MODEL_SIZE=${WHISPER_MODEL_SIZE}${EXTRA_CONF} --conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.defaultCatalog=${CATALOG_NAME} --conf spark.sql.catalog.${CATALOG_NAME}=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.${CATALOG_NAME}.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.${CATALOG_NAME}.warehouse=s3://${BUCKET_NAME}/warehouse --conf spark.sql.catalog.${CATALOG_NAME}.io-impl=org.apache.iceberg.aws.s3.S3FileIO\"
      }
    }" \
    --configuration-overrides "{
      \"monitoringConfiguration\": {
        \"s3MonitoringConfiguration\": {\"logUri\": \"s3://${BUCKET_NAME}/logs/\"}
      }
    }" \
    --query 'jobRunId' --output text)

echo "Started job run: $JOB_RUN_ID"
echo "Check progress: aws emr-serverless get-job-run --application-id $APP_ID --job-run-id $JOB_RUN_ID"
echo "Logs: s3://${BUCKET_NAME}/logs/"
