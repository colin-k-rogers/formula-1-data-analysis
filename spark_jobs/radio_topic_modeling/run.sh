#!/usr/bin/env bash
# Submits an EMR Serverless job run:
#   ./run.sh
# Re-run any time you want to pick up a newly-run race weekend's radio —
# it's incremental (see README.md). Set FORCE_REFIT=true,
# REPROCESS_SESSIONS="<comma-separated session_key list>", or
# REPROCESS_ALL=true (every session in SEASON_YEAR) in the environment
# before calling this script to opt into those, e.g.:
#   FORCE_REFIT=true ./run.sh
#   SEASON_YEAR=2024 REPROCESS_ALL=true ./run.sh
#
# STAGING=true ./run.sh submits against the staging application (state.sh's
# STAGING_APP_ID, written by ./setup_staging.sh) instead of production, and
# writes to the `stg_raw` Iceberg namespace and a separate MODEL_STORE_PATH
# so it can never collide with production's tables or persisted BERTopic
# model — see README.md's "PR staging" section for why and the end-to-end
# testing flow this enables.
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
source ./state.sh   # written by setup.sh: APP_ID, VPC_ID, SUBNET_ID, SG_ID (+ STAGING_APP_ID if ./setup_staging.sh has run)

# Any non-empty value opts in, matching FORCE_REFIT/REPROCESS_SESSIONS/
# REPROCESS_ALL below (not a strict "true" match) -- getting this wrong
# should fail toward staging, not silently fall through to production.
if [ -n "${STAGING:-}" ]; then
    if [ -z "${STAGING_APP_ID:-}" ]; then
        echo "STAGING is set but state.sh has no STAGING_APP_ID — run ./setup_staging.sh first." >&2
        exit 1
    fi
    APP_ID="$STAGING_APP_ID"
    MODEL_STORE_PATH="s3://${BUCKET_NAME}/staging/models/bertopic/model.tar.gz"
    ICEBERG_NAMESPACE="stg_raw"
else
    MODEL_STORE_PATH="s3://${BUCKET_NAME}/models/bertopic/model.tar.gz"
    # .env may define ICEBERG_NAMESPACE (it's also job.py's own config knob
    # for a standalone run) -- clear it for a plain production run so it's
    # never forwarded below, preserving this script's pre-STAGING behavior
    # of always relying on job.py's own "raw" default in that case.
    ICEBERG_NAMESPACE=""
fi
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/f1-radio-topics-emrs-role"
# Falls back to the Dockerfile/setup.sh default if .env predates this
# variable -- must match whatever size the image was actually built with
# (see setup.sh's --build-arg), since this only controls what the running
# job requests, not what's pre-baked.
WHISPER_MODEL_SIZE="${WHISPER_MODEL_SIZE:-medium}"

EXTRA_CONF=""
if [ -n "${ICEBERG_NAMESPACE:-}" ]; then
    EXTRA_CONF+=" --conf spark.emr-serverless.driverEnv.ICEBERG_NAMESPACE=${ICEBERG_NAMESPACE}"
fi
if [ -n "${FORCE_REFIT:-}" ]; then
    EXTRA_CONF+=" --conf spark.emr-serverless.driverEnv.FORCE_REFIT=${FORCE_REFIT}"
fi
if [ -n "${REPROCESS_SESSIONS:-}" ]; then
    EXTRA_CONF+=" --conf spark.emr-serverless.driverEnv.REPROCESS_SESSIONS=${REPROCESS_SESSIONS}"
fi
if [ -n "${REPROCESS_ALL:-}" ]; then
    EXTRA_CONF+=" --conf spark.emr-serverless.driverEnv.REPROCESS_ALL=${REPROCESS_ALL}"
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

if [ -n "${STAGING:-}" ]; then
    echo "Started STAGING job run: $JOB_RUN_ID (application $APP_ID, namespace stg_raw)"
else
    echo "Started job run: $JOB_RUN_ID"
fi
echo "Check progress: aws emr-serverless get-job-run --application-id $APP_ID --job-run-id $JOB_RUN_ID"
echo "Logs: s3://${BUCKET_NAME}/logs/"
