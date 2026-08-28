# Radio topic modeling — Spark job (AWS EMR Serverless + Glue Catalog)

Transcribes F1 team-radio audio with Whisper and topic-models the transcripts
with BERTopic, writing the results as Iceberg tables registered in the AWS
Glue Data Catalog that MotherDuck attaches directly (see
[../../warehouse/setup_radio_lakehouse.sql](../../warehouse/setup_radio_lakehouse.sql)).

Unlike the two Flights in [flights/](../../flights), this does **not** run
inside MotherDuck — it needs a real Spark runtime and Whisper/BERTopic model
weights, which don't fit a lightweight Flight. It runs as an
[AWS EMR Serverless](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/)
job run, submitted on demand via the AWS CLI (not on a MotherDuck schedule).

Every `<ACCOUNT_ID>` / `<BUCKET_NAME>` / region below is a placeholder — pick
your own names and substitute them consistently through this whole doc.
Check `aws emr-serverless list-release-labels --region <REGION>` for the
current EMR release before you start — `emr-7.5.0` below may not be the
latest by the time you read this.

## One-time AWS setup (from scratch)

```bash
ACCOUNT_ID="123456789012"
REGION="us-east-1"
BUCKET_NAME="f1-radio-topics-lakehouse"
REPO_NAME="f1-radio-topics"
IMAGE_TAG="latest"
EMR_RELEASE="emr-7.5.0"
CATALOG_NAME="radio"          # matches ICEBERG_CATALOG_NAME in job.py
ICEBERG_NAMESPACE="raw"       # matches ICEBERG_NAMESPACE in job.py — this
                               # becomes the Glue database name
MODEL_STORE_PATH="s3://${BUCKET_NAME}/models/bertopic/model.tar.gz"
IMAGE="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPO_NAME}:${IMAGE_TAG}"

# 1. S3 bucket — holds the Iceberg warehouse data, EMR Serverless logs, and
#    the persisted BERTopic model (MODEL_STORE_PATH below) — all under one
#    bucket so the IAM policy in step 3 already covers all of it.
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
```

## Running the job

```bash
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/f1-radio-topics-emrs-role"

aws emr-serverless start-job-run \
    --application-id "$APP_ID" \
    --execution-role-arn "$ROLE_ARN" \
    --job-driver "{
      \"sparkSubmit\": {
        \"entryPoint\": \"local:///opt/radio_topic_modeling/job.py\",
        \"sparkSubmitParameters\": \"--conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=/opt/venv/bin/python --conf spark.executorEnv.PYSPARK_PYTHON=/opt/venv/bin/python --conf spark.emr-serverless.driverEnv.SEASON_YEAR=2025 --conf spark.emr-serverless.driverEnv.MODEL_STORE_PATH=${MODEL_STORE_PATH} --conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.defaultCatalog=${CATALOG_NAME} --conf spark.sql.catalog.${CATALOG_NAME}=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.${CATALOG_NAME}.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.${CATALOG_NAME}.warehouse=s3://${BUCKET_NAME}/warehouse --conf spark.sql.catalog.${CATALOG_NAME}.io-impl=org.apache.iceberg.aws.s3.S3FileIO\"
      }
    }" \
    --configuration-overrides "{
      \"monitoringConfiguration\": {
        \"s3MonitoringConfiguration\": {\"logUri\": \"s3://${BUCKET_NAME}/logs/\"}
      }
    }"
```

Re-run this any time you want to pick up a newly-run race weekend's radio.
It's incremental: a run only fetches/transcribes sessions it hasn't already
processed, and assigns their topics into the topic space persisted at
`MODEL_STORE_PATH` — so topic ids/labels for prior races stay stable across
runs instead of reshuffling every time. Two escape hatches for the cases
that don't fit that default:
- `REPROCESS_SESSIONS=<comma-separated session_key list>` re-fetches and
  re-transcribes specific sessions even though they're already in the
  table (e.g. OpenF1 published a correction), still using the existing
  topic space.
- `FORCE_REFIT=true` refits BERTopic from scratch over every transcript
  ever produced (not just this run's) and rewrites every row's topic
  assignment to match — a deliberate, rare action for when the season's
  accumulated enough new data that the topic space itself should change,
  not something to set on every run.

Check progress with `aws emr-serverless get-job-run --application-id "$APP_ID" --job-run-id <job-run-id>`,
and the Spark driver/executor logs under `s3://${BUCKET_NAME}/logs/`.

## Config knobs

`job.py` only imports once per process, so each variable must be set as an
env var on whichever side (driver or executor) actually reads it —
`spark.emr-serverless.driverEnv.<NAME>=<value>` for the driver,
`spark.executorEnv.<NAME>=<value>` for executors. Only `WHISPER_MODEL_SIZE`
runs on executors (`transcribe_partition`); everything else below runs on
the driver (`main` and the functions it calls). `WHISPER_MODEL_SIZE` and
`EMBEDDING_MODEL_NAME` can additionally be pinned at image-build time via
the matching `--build-arg` (so the right model weights get pre-baked) — the
env var still needs to be set at submit time to match, since the `ARG`
only controls what's baked in, not what the running job requests:

| Variable | Default | Scope | Purpose |
|---|---|---|---|
| `SEASON_YEAR` | `2025` | driver | Which season's Race sessions to pull team radio for |
| `WHISPER_MODEL_SIZE` | `base` | executor | faster-whisper model size (`tiny`/`base`/`small`/`medium`/`large-v3`) — bigger = more accurate, slower, must match the `--build-arg` used when building the image so the weights are pre-baked |
| `EMBEDDING_MODEL_NAME` | `all-MiniLM-L6-v2` | driver | sentence-transformers model BERTopic embeds transcripts with |
| `ICEBERG_CATALOG_NAME` | `radio` | driver | Must match `${CATALOG_NAME}` used in the `spark.sql.catalog.*` properties above |
| `ICEBERG_NAMESPACE` | `raw` | driver | Iceberg namespace (= Glue database name) the two output tables are created under |
| `MODEL_STORE_PATH` | unset | driver | S3 path (e.g. `s3://bucket/models/bertopic/model.tar.gz`) where the fitted BERTopic model is persisted so later runs can `transform()` new documents into the same topic space instead of refitting. Leaving it unset means every run behaves like `FORCE_REFIT=true` — there's nothing to load, so it fits fresh every time |
| `FORCE_REFIT` | `false` | driver | Refit BERTopic from scratch over every transcript ever produced and rewrite every row's topic assignment — a deliberate, rare action, not a default |
| `REPROCESS_SESSIONS` | unset | driver | Comma-separated `session_key` list to re-fetch/re-transcribe even though already processed (e.g. an OpenF1 correction), using the existing topic space |

## Output

Two Iceberg tables under the Glue database `${CATALOG_NAME}.${ICEBERG_NAMESPACE}`
(i.e. a Glue database named `raw`):

- `radio_messages` — one row per team-radio call: `radio_message_id`,
  `session_key`, `meeting_key`, `driver_number`, `message_date`,
  `recording_url`, `transcript_text`, `language`, `duration_sec`,
  `transcribe_error`, `topic_id`, `topic_probability`.
- `radio_topics` — one row per BERTopic topic: `topic_id`, `label`,
  `top_keywords`, `doc_count`.

From here, [../../warehouse/setup_radio_lakehouse.sql](../../warehouse/setup_radio_lakehouse.sql)
attaches this Glue catalog into MotherDuck so dbt can read these tables
directly as a source.

## Cost

This is season-scale data (~1,000 short radio clips total), so the very
first run — a full-season backfill — costs on the order of **$0.10–$1 in
EMR Serverless compute** (a handful of small workers for ~10–20 minutes).
Every run after that is incremental (one new race weekend's ~30–50 clips),
so it's a fraction of that — likely a few cents, dominated more by fixed
job-startup overhead than actual compute. Call it a few dollars total for a
full season of weekly runs. Glue Data Catalog and S3 storage (including the
persisted model) stay within or close to the free tier at this scale. The
one standing cost regardless of how often you run it is **ECR image
storage** for the custom container (~3–5GB, ~$0.40–0.50/month) — delete the
image between seasons if you're not actively iterating on it. Using a
public subnet instead of a NAT Gateway (see step 2 above) avoids the one
AWS networking cost that could otherwise dominate this estimate.
