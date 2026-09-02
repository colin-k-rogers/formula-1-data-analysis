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

```bash
cp .env.example .env
```

Every value in [.env.example](.env.example) is a placeholder — edit your
copy with your own account id, bucket name, and region before running
either script below (`.env` itself is git-ignored, same as any other
`.env` in this repo). Check `aws emr-serverless list-release-labels
--region <REGION>` for the current EMR release — `emr-7.5.0` in there may
not be the latest by the time you read this.

If you use named AWS CLI profiles, uncomment `AWS_PROFILE` in `.env` — both
scripts `source` the file with `set -a`, which exports every variable it
sets, so the `aws`/`docker login` calls in `setup.sh` and `run.sh` pick it
up automatically without needing a `--profile` flag anywhere.

## One-time AWS setup (from scratch)

```bash
./setup.sh
```

[setup.sh](setup.sh) creates, in order: the S3 bucket (Iceberg warehouse
data, job logs, and the persisted BERTopic model all live under it); a VPC
with a **public** subnet — EMR Serverless without a VPC can only reach
S3/Glue/CloudWatch/STS/KMS/DynamoDB/Secrets Manager in-region, not the
public internet this job needs to reach the OpenF1 API, and a public subnet
gets you that without paying for a NAT Gateway; the IAM job runtime role
(S3 + Glue permissions); the ECR repo, custom container image build/push,
and the repository policy that lets EMR Serverless pull it; and finally the
EMR Serverless application itself, wired to that VPC and image.

It's safe to re-run: every resource is looked up by a fixed name/tag first
and only created if missing (the EMR Serverless application's image/network
config is updated in place instead — the point of rerunning after pushing a
new image). The one exception is the IAM policy's content, which isn't
diffed and updated on rerun — see the comment in `setup.sh`. It writes the
resource ids it finds/creates (`VPC_ID`, `SUBNET_ID`, `SG_ID`, `APP_ID`) to
`state.sh`, which is git-ignored and which `run.sh` reads.

## Running the job

```bash
./run.sh
```

Re-run this any time you want to pick up a newly-run race weekend's radio.
It's incremental: a run only fetches/transcribes sessions it hasn't already
processed, and assigns their topics into the topic space persisted at
`MODEL_STORE_PATH` — so topic ids/labels for prior races stay stable across
runs instead of reshuffling every time. Two escape hatches for the cases
that don't fit that default, both read from the environment by
[run.sh](run.sh):

```bash
# Re-fetch/re-transcribe specific sessions even though they're already in
# the table (e.g. OpenF1 published a correction), still using the existing
# topic space.
REPROCESS_SESSIONS="9987,9988" ./run.sh

# Refit BERTopic from scratch over every transcript ever produced (not
# just this run's) and rewrite every row's topic assignment to match — a
# deliberate, rare action for when the season's accumulated enough new
# data that the topic space itself should change, not something to set on
# every run.
FORCE_REFIT=true ./run.sh
```

`run.sh` prints the job run id and the exact `aws emr-serverless
get-job-run` command to check on it; logs land under `s3://${BUCKET_NAME}/logs/`.

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
| `WHISPER_MODEL_SIZE` | `medium` | executor | faster-whisper model size (`tiny`/`base`/`small`/`medium`/`large-v3`) — bigger = more accurate, slower, must match the `--build-arg` used when building the image so the weights are pre-baked |
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
public subnet instead of a NAT Gateway (see `setup.sh`'s networking step)
avoids the one AWS networking cost that could otherwise dominate this
estimate.
