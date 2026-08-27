# Radio topic modeling — Spark job (GCP Managed Service for Apache Spark)

Transcribes F1 team-radio audio with Whisper and topic-models the transcripts
with BERTopic, writing the results as Iceberg tables that MotherDuck attaches
directly (see [../../warehouse/setup_radio_lakehouse.sql](../../warehouse/setup_radio_lakehouse.sql)).

Unlike the two Flights in [flights/](../../flights), this does **not** run
inside MotherDuck — it needs a real Spark runtime and Whisper/BERTopic model
weights, which don't fit a lightweight Flight. It runs as a
[Google Cloud Managed Service for Apache Spark](https://docs.cloud.google.com/managed-spark)
serverless batch, submitted on demand via `gcloud` (not on a MotherDuck
schedule).

Every `PROJECT_ID` / `BUCKET_NAME` / region below is a placeholder — pick
your own names and substitute them consistently through this whole doc.

## One-time GCP setup (from scratch)

```bash
PROJECT_ID="f1-radio-topics"
REGION="us-central1"
BUCKET_NAME="f1-radio-topics-lakehouse"
REPO_NAME="f1-radio-topics"
IMAGE_TAG="latest"
CATALOG_NAME="radio"          # matches ICEBERG_CATALOG_NAME in job.py
ICEBERG_NAMESPACE="raw"       # matches ICEBERG_NAMESPACE in job.py

# 1. Project + billing (skip if you already have a project — link a billing
#    account to it in the console, gcloud can't do that step for you).
gcloud projects create "$PROJECT_ID"
gcloud config set project "$PROJECT_ID"

# 2. Enable the APIs this pipeline needs.
gcloud services enable \
    dataproc.googleapis.com \
    biglake.googleapis.com \
    storage.googleapis.com \
    artifactregistry.googleapis.com

# 3. GCS bucket — holds both the Iceberg warehouse and Spark staging files.
gsutil mb -l "$REGION" "gs://$BUCKET_NAME/"

# 4. Grant the Compute Engine default service account (used by Managed Spark
#    batches) the roles it needs to run jobs and write to the catalog.
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')
for ROLE in roles/dataproc.worker roles/serviceusage.serviceUsageConsumer \
            roles/biglake.editor roles/bigquery.dataEditor; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
      --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
      --role="$ROLE"
done

# 5. Create the Lakehouse Iceberg REST catalog over the bucket.
#    As of this writing this step is console-only:
#      console.cloud.google.com/biglake -> "Create catalog"
#      -> "Iceberg REST catalog" -> "Cloud Storage bucket" -> pick $BUCKET_NAME
#      -> auth mode "Credential vending" -> confirm bucket permissions.
#    Catalog permission propagation is eventually consistent — if the first
#    batch run gets 403s from the REST catalog, wait a few minutes and retry.

# 6. Artifact Registry repo + build/push the custom container image (needed
#    because faster-whisper/BERTopic/torch are too heavy to pip-install on
#    every batch run — see ./Dockerfile).
gcloud artifacts repositories create "$REPO_NAME" \
    --repository-format=docker --location="$REGION"

gcloud auth configure-docker "${REGION}-docker.pkg.dev"

IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/radio-topic-modeling:${IMAGE_TAG}"
docker build -t "$IMAGE" .
docker push "$IMAGE"

# 7. Stage job.py where Managed Spark can read it.
gsutil cp job.py "gs://$BUCKET_NAME/job.py"
```

## Running the job

```bash
gcloud dataproc batches submit pyspark "gs://$BUCKET_NAME/job.py" \
    --project="$PROJECT_ID" \
    --region="$REGION" \
    --version=2.2 \
    --container-image="$IMAGE" \
    --properties="\
spark.sql.defaultCatalog=${CATALOG_NAME},\
spark.sql.catalog.${CATALOG_NAME}=org.apache.iceberg.spark.SparkCatalog,\
spark.sql.catalog.${CATALOG_NAME}.type=rest,\
spark.sql.catalog.${CATALOG_NAME}.uri=https://biglake.googleapis.com/iceberg/v1/restcatalog,\
spark.sql.catalog.${CATALOG_NAME}.warehouse=gs://${BUCKET_NAME},\
spark.sql.catalog.${CATALOG_NAME}.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO,\
spark.sql.catalog.${CATALOG_NAME}.header.x-goog-user-project=${PROJECT_ID},\
spark.sql.catalog.${CATALOG_NAME}.rest.auth.type=org.apache.iceberg.gcp.auth.GoogleAuthManager,\
spark.sql.catalog.${CATALOG_NAME}.header.X-Iceberg-Access-Delegation=vended-credentials,\
spark.sql.catalog.${CATALOG_NAME}.gcs.oauth2.refresh-credentials-endpoint=https://oauth2.googleapis.com/token,\
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
    --properties="^;^spark.executorEnv.SEASON_YEAR=2025"
```

Re-run this any time you want to pick up a newly-run race weekend's radio —
it's idempotent per `session_key` (see `write_iceberg_tables` in `job.py`),
but note that because BERTopic is refit over *all* transcripts every run,
topic ids/labels for prior races can shift slightly each time you re-run.

## Config knobs

Set as `spark.executorEnv.<NAME>=<value>` / `spark.driverEnv.<NAME>=<value>`
in `--properties`, or bake into the image at build time via the matching
`--build-arg`:

| Variable | Default | Purpose |
|---|---|---|
| `SEASON_YEAR` | `2025` | Which season's Race sessions to pull team radio for |
| `WHISPER_MODEL_SIZE` | `base` | faster-whisper model size (`tiny`/`base`/`small`/`medium`/`large-v3`) — bigger = more accurate, slower, must match the `--build-arg` used when building the image so the weights are pre-baked |
| `EMBEDDING_MODEL_NAME` | `all-MiniLM-L6-v2` | sentence-transformers model BERTopic embeds transcripts with |
| `ICEBERG_CATALOG_NAME` | `radio` | Must match `${CATALOG_NAME}` used in the `spark.sql.catalog.*` properties above |
| `ICEBERG_NAMESPACE` | `raw` | Iceberg namespace the two output tables are created under |

## Output

Two Iceberg tables under `${CATALOG_NAME}.${ICEBERG_NAMESPACE}`:

- `radio_messages` — one row per team-radio call: `radio_message_id`,
  `session_key`, `meeting_key`, `driver_number`, `message_date`,
  `recording_url`, `transcript_text`, `language`, `duration_sec`,
  `transcribe_error`, `topic_id`, `topic_probability`.
- `radio_topics` — one row per BERTopic topic: `topic_id`, `label`,
  `top_keywords`, `doc_count`.

From here, [../../warehouse/setup_radio_lakehouse.sql](../../warehouse/setup_radio_lakehouse.sql)
attaches this catalog into MotherDuck so dbt can read these tables directly
as a source.
