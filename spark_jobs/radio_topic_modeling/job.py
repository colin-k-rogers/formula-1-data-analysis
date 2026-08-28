"""AWS EMR Serverless batch: transcribe F1 team-radio audio with Whisper and
topic-model the transcripts with BERTopic.

This is NOT a MotherDuck Flight — it needs a real Spark runtime plus GPU/CPU
time and model weights that don't fit MotherDuck's lightweight Flight runner.
Submit it as an EMR Serverless job run (see README.md in this directory for
the one-time AWS setup and the exact `aws emr-serverless start-job-run`
command). Output lands as two Iceberg tables registered in the AWS Glue Data
Catalog (S3-backed), which MotherDuck attaches as the `radio_lakehouse`
database (see warehouse/setup_radio_lakehouse.sql) — dbt reads them directly
from there, no copy step needed.

The catalog/warehouse specifics (Glue, S3) live entirely in spark-submit
config and the container image, not in this file — the code below only ever
addresses Iceberg tables by name (`${CATALOG_NAME}.${ICEBERG_NAMESPACE}.*`),
so it doesn't need to change if the catalog backend does.

Incremental by default: a run only fetches/transcribes sessions that aren't
already in radio_messages (or are explicitly listed in REPROCESS_SESSIONS),
and assigns their topics with `BERTopic.transform()` against a model
persisted at MODEL_STORE_PATH from the last fit — not a fresh `fit_transform`
every time. Refitting from scratch every run would reshuffle topic ids/labels
for every prior race too, which defeats the point of tracking how topics
evolve *across* races. FORCE_REFIT=true opts into that reshuffle as a
deliberate, rare action instead (e.g. once a season has enough data for a
better topic model) rather than a side effect of every run.

Writes are atomic MERGE INTO / createOrReplace() operations (single Iceberg
commit each), not DELETE-then-INSERT or DROP-then-CREATE as separate
statements, so a crash mid-run can't leave the table missing rows it already
had, or missing entirely.
"""
import io
import os
import tarfile
import tempfile
import time
from datetime import datetime

import boto3
import botocore.exceptions
import pandas as pd
import requests
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BASE_URL = "https://api.openf1.org/v1"
REQUEST_TIMEOUT_SEC = 30
MAX_RETRIES = 3
MAX_RATE_LIMIT_RETRIES = 6
RETRY_BACKOFF_SEC = 2
RATE_LIMIT_BACKOFF_SEC = 5
INTER_REQUEST_SLEEP_SEC = 0.5

WHISPER_MODEL_SIZE = os.environ.get("WHISPER_MODEL_SIZE", "base")
EMBEDDING_MODEL_NAME = os.environ.get("EMBEDDING_MODEL_NAME", "all-MiniLM-L6-v2")
CATALOG_NAME = os.environ.get("ICEBERG_CATALOG_NAME", "radio")
ICEBERG_NAMESPACE = os.environ.get("ICEBERG_NAMESPACE", "raw")
# Where the fitted BERTopic model is persisted between runs, e.g.
# s3://bucket/models/bertopic/model.tar.gz. Without this set, every run
# fits fresh (same as FORCE_REFIT=true) since there's nothing to load.
MODEL_STORE_PATH = os.environ.get("MODEL_STORE_PATH")
FORCE_REFIT = os.environ.get("FORCE_REFIT", "false").lower() == "true"
REPROCESS_SESSIONS = {
    int(s) for s in os.environ.get("REPROCESS_SESSIONS", "").split(",") if s.strip()
}


def fetch(endpoint, params):
    """GET with retries; mirrors flights/ingest_openf1/main.py's fetch()."""
    url = f"{BASE_URL}/{endpoint}"
    last_err = None
    retries_used = 0
    rate_limit_retries_used = 0
    while retries_used < MAX_RETRIES and rate_limit_retries_used < MAX_RATE_LIMIT_RETRIES:
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SEC)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as err:
            last_err = err
            status = err.response.status_code if err.response is not None else None
            if status == 429:
                retry_after = err.response.headers.get("Retry-After")
                sleep_sec = float(retry_after) if retry_after else RATE_LIMIT_BACKOFF_SEC * (2 ** rate_limit_retries_used)
                rate_limit_retries_used += 1
            else:
                retries_used += 1
                sleep_sec = RETRY_BACKOFF_SEC * retries_used
            time.sleep(sleep_sec)
    total_attempts = 1 + retries_used + rate_limit_retries_used
    raise RuntimeError(f"GET {url} params={params} failed after {total_attempts} attempts") from last_err


def fetch_race_sessions(season_year):
    """All Race sessions for `season_year` (metadata only — cheap, and safe
    to call every run since it's just used to discover which sessions
    exist, not to fetch their radio)."""
    all_race_type_sessions = fetch("sessions", {"year": season_year, "session_type": "Race"})
    return [s for s in all_race_type_sessions if s.get("session_name") == "Race"]


def fetch_team_radio_for_sessions(sessions):
    """Team-radio metadata (recording_url + who/when) for exactly the given
    sessions, so a run only pays for OpenF1 calls on sessions it's actually
    going to transcribe."""
    radio_messages = []
    for session in sessions:
        session_key = session["session_key"]
        messages = fetch("team_radio", {"session_key": session_key})
        for m in messages:
            m["meeting_key"] = session["meeting_key"]
        radio_messages.extend(messages)
        time.sleep(INTER_REQUEST_SLEEP_SEC)
    return radio_messages


RADIO_METADATA_SCHEMA = StructType([
    StructField("radio_message_id", StringType(), False),
    StructField("session_key", IntegerType(), False),
    StructField("meeting_key", IntegerType(), False),
    StructField("driver_number", IntegerType(), False),
    StructField("message_date", TimestampType(), True),
    StructField("recording_url", StringType(), False),
])

TRANSCRIPT_SCHEMA = StructType(
    RADIO_METADATA_SCHEMA.fields
    + [
        StructField("transcript_text", StringType(), True),
        StructField("language", StringType(), True),
        StructField("duration_sec", DoubleType(), True),
        StructField("transcribe_error", StringType(), True),
    ]
)

TOPICS_SCHEMA = StructType([
    StructField("topic_id", IntegerType(), False),
    StructField("label", StringType(), True),
    StructField("top_keywords", StringType(), True),
    StructField("doc_count", IntegerType(), True),
])

ASSIGNMENTS_SCHEMA = StructType([
    StructField("radio_message_id", StringType(), False),
    StructField("topic_id", IntegerType(), False),
    StructField("topic_probability", DoubleType(), True),
])


def transcribe_partition(rows):
    """Runs once per Spark partition: loads the Whisper model a single time,
    then downloads + transcribes every clip assigned to this partition.
    Loading per-partition (not per-row) is what makes this worth
    distributing across executors instead of running serially."""
    from faster_whisper import WhisperModel

    model = WhisperModel(WHISPER_MODEL_SIZE, device="cpu", compute_type="int8")

    for row in rows:
        d = row.asDict()
        try:
            resp = requests.get(d["recording_url"], timeout=REQUEST_TIMEOUT_SEC)
            resp.raise_for_status()
            audio_buf = io.BytesIO(resp.content)
            segments, info = model.transcribe(audio_buf, beam_size=5)
            text = " ".join(seg.text.strip() for seg in segments).strip()
            d["transcript_text"] = text or None
            d["language"] = info.language
            d["duration_sec"] = info.duration
            d["transcribe_error"] = None
        except Exception as err:  # noqa: BLE001 - one bad clip shouldn't fail the batch
            d["transcript_text"] = None
            d["language"] = None
            d["duration_sec"] = None
            d["transcribe_error"] = str(err)[:500]
        yield Row(**d)


def _split_s3_uri(uri):
    bucket, _, key = uri[len("s3://"):].partition("/")
    return bucket, key


def load_persisted_topic_model(embedding_model):
    """Loads the BERTopic model a prior run saved to MODEL_STORE_PATH, so
    this run can assign new documents into that SAME topic space via
    transform() instead of fitting a new, incompatible one. Returns None if
    MODEL_STORE_PATH isn't configured or nothing's been saved yet (the
    caller treats that the same as "first run ever": fit fresh)."""
    if not MODEL_STORE_PATH:
        return None
    from bertopic import BERTopic

    bucket, key = _split_s3_uri(MODEL_STORE_PATH)
    with tempfile.TemporaryDirectory() as tmp:
        archive_path = os.path.join(tmp, "model.tar.gz")
        try:
            boto3.client("s3").download_file(bucket, key, archive_path)
        except botocore.exceptions.ClientError as err:
            if err.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
                return None
            raise
        extract_dir = os.path.join(tmp, "model")
        with tarfile.open(archive_path) as tar:
            tar.extractall(extract_dir)
        return BERTopic.load(extract_dir, embedding_model=embedding_model)


def save_topic_model(topic_model):
    """Persists a freshly fit BERTopic model to MODEL_STORE_PATH for a
    later run's load_persisted_topic_model() to pick up. No-op if
    MODEL_STORE_PATH isn't configured (every run just fits fresh instead)."""
    if not MODEL_STORE_PATH:
        return
    bucket, key = _split_s3_uri(MODEL_STORE_PATH)
    with tempfile.TemporaryDirectory() as tmp:
        save_dir = os.path.join(tmp, "model")
        # save_embedding_model=False: the embedding model's weights are
        # already baked into this job's container image (see Dockerfile),
        # no need to duplicate them into every saved snapshot.
        topic_model.save(save_dir, serialization="safetensors", save_embedding_model=False)
        archive_path = os.path.join(tmp, "model.tar.gz")
        with tarfile.open(archive_path, "w:gz") as tar:
            tar.add(save_dir, arcname=".")
        boto3.client("s3").upload_file(archive_path, bucket, key)


def _assignments_frame(radio_message_ids, topics, probabilities):
    # fit_transform/transform's docs describe probabilities as 2D (every
    # topic's probability for every document), but it comes back 1D
    # (already just the assigned topic's probability) when clustering
    # collapses to very few topics — handle both shapes rather than assume.
    if probabilities is not None and getattr(probabilities, "ndim", 1) == 2:
        topic_probability = probabilities.max(axis=1)
    else:
        topic_probability = probabilities
    return pd.DataFrame({
        "radio_message_id": pd.Series(radio_message_ids).reset_index(drop=True),
        "topic_id": topics,
        "topic_probability": topic_probability,
    })


def fit_topics_fresh(docs_pdf, embedding_model):
    """Fits a brand-new BERTopic model over `docs_pdf` (radio_message_id,
    transcript_text) — used for the very first run ever (nothing to load
    from MODEL_STORE_PATH yet) and for a deliberate FORCE_REFIT. Returns
    (assignments, topics, fitted_model)."""
    from bertopic import BERTopic

    topic_model = BERTopic(embedding_model=embedding_model, calculate_probabilities=True)
    topics, probabilities = topic_model.fit_transform(docs_pdf["transcript_text"].tolist())
    assignments = _assignments_frame(docs_pdf["radio_message_id"], topics, probabilities)

    topic_info = topic_model.get_topic_info()  # columns: Topic, Count, Name, Representation, ...
    topics_out = pd.DataFrame({
        "topic_id": topic_info["Topic"],
        "label": topic_info["Name"],
        "top_keywords": topic_info["Representation"].apply(lambda kws: ", ".join(kws[:8])),
        "doc_count": topic_info["Count"],
    })
    return assignments, topics_out, topic_model


def transform_topics(topic_model, docs_pdf):
    """Assigns `docs_pdf` into an already-fitted model's EXISTING topic
    space, instead of refitting — this is what keeps topic ids/labels
    stable across runs in the common (non-FORCE_REFIT) case."""
    topics, probabilities = topic_model.transform(docs_pdf["transcript_text"].tolist())
    return _assignments_frame(docs_pdf["radio_message_id"], topics, probabilities)


def write_new_messages(spark, new_rows_df, messages_table):
    """Idempotent upsert of this run's freshly transcribed rows — a single
    atomic MERGE, not a DELETE-then-INSERT, so a crash mid-run never leaves
    the table without rows it already committed. Deliberately never
    touches topic_id/topic_probability on an existing (reprocessed) row
    here — update_topic_assignments() is what's allowed to change those,
    so a row being reprocessed keeps its last-known-good topic assignment
    right up until its new one is ready, instead of going through a
    visible "topic unknown" gap between the two merges below."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.{ICEBERG_NAMESPACE}")

    if not spark.catalog.tableExists(messages_table):
        new_rows_df.writeTo(messages_table).using("iceberg").create()
        return

    new_rows_df.createOrReplaceTempView("new_radio_messages")
    non_topic_cols = [c for c in new_rows_df.columns if c not in ("radio_message_id", "topic_id", "topic_probability")]
    update_cols = ", ".join(f"{c} = s.{c}" for c in non_topic_cols)
    insert_cols = ", ".join(new_rows_df.columns)
    insert_vals = ", ".join(f"s.{c}" for c in new_rows_df.columns)
    spark.sql(f"""
        MERGE INTO {messages_table} t
        USING new_radio_messages s
        ON t.radio_message_id = s.radio_message_id
        WHEN MATCHED THEN UPDATE SET {update_cols}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)


def update_topic_assignments(spark, assignments_df, messages_table):
    """Atomically fills in topic_id/topic_probability for exactly the rows
    in `assignments_df` — covers the whole corpus on a fresh fit, or just
    this run's new/reprocessed rows on an incremental transform."""
    assignments_df.createOrReplaceTempView("new_topic_assignments")
    spark.sql(f"""
        MERGE INTO {messages_table} t
        USING new_topic_assignments s
        ON t.radio_message_id = s.radio_message_id
        WHEN MATCHED THEN UPDATE SET t.topic_id = s.topic_id, t.topic_probability = s.topic_probability
    """)


def main():
    season_year = os.environ.get("SEASON_YEAR", "2025")

    spark = SparkSession.builder.appName("f1-radio-topic-modeling").getOrCreate()

    messages_table = f"{CATALOG_NAME}.{ICEBERG_NAMESPACE}.radio_messages"
    topics_table = f"{CATALOG_NAME}.{ICEBERG_NAMESPACE}.radio_topics"
    table_existed_before_this_run = spark.catalog.tableExists(messages_table)

    already_processed = set()
    if table_existed_before_this_run:
        already_processed = {
            row["session_key"]
            for row in spark.sql(f"SELECT DISTINCT session_key FROM {messages_table}").collect()
        }

    all_sessions = fetch_race_sessions(season_year)
    if not all_sessions:
        print(f"No Race sessions found for season {season_year}; nothing to do.")
        return

    sessions_to_process = [
        s for s in all_sessions
        if s["session_key"] not in already_processed or s["session_key"] in REPROCESS_SESSIONS
    ]
    if not sessions_to_process:
        print(f"season={season_year}: every session already processed, nothing new to transcribe.")
        return

    radio_metadata = fetch_team_radio_for_sessions(sessions_to_process)
    if not radio_metadata:
        print(f"season={season_year}: no team radio in the {len(sessions_to_process)} session(s) processed this run.")
        return

    for m in radio_metadata:
        m["radio_message_id"] = f"{m['session_key']}:{m['driver_number']}:{m['date']}"
        # OpenF1 dates are ISO 8601 strings; Spark's TimestampType schema
        # needs actual datetime objects, not strings, to convert cleanly.
        m["message_date"] = datetime.fromisoformat(m.pop("date").replace("Z", "+00:00"))

    metadata_rows = [
        {k: m.get(k) for k in RADIO_METADATA_SCHEMA.fieldNames()} for m in radio_metadata
    ]
    metadata_df = spark.createDataFrame(metadata_rows, schema=RADIO_METADATA_SCHEMA)

    # Repartition so each partition gets a manageable, roughly even batch of
    # clips to download+transcribe; too many partitions wastes model-load
    # time, too few serializes the run onto a handful of executors.
    num_partitions = max(1, min(32, len(metadata_rows) // 10 or 1))
    transcribed_rdd = metadata_df.repartition(num_partitions).rdd.mapPartitions(transcribe_partition)
    transcripts_df = spark.createDataFrame(transcribed_rdd, schema=TRANSCRIPT_SCHEMA)
    new_rows_df = (
        transcripts_df
        .withColumn("topic_id", F.lit(None).cast(IntegerType()))
        .withColumn("topic_probability", F.lit(None).cast(DoubleType()))
    )
    new_rows_df.cache()
    n_new_messages = new_rows_df.count()

    # Land this run's transcripts first (topic assignment filled in below).
    # For FORCE_REFIT this also makes them visible to the "read the whole
    # corpus back" query that follows.
    write_new_messages(spark, new_rows_df, messages_table)

    if FORCE_REFIT:
        docs_pdf = spark.sql(
            f"SELECT radio_message_id, transcript_text FROM {messages_table} WHERE transcript_text IS NOT NULL"
        ).toPandas()
    else:
        docs_pdf = new_rows_df.filter("transcript_text IS NOT NULL").select("radio_message_id", "transcript_text").toPandas()

    if docs_pdf.empty:
        print(f"season={season_year} sessions_processed={len(sessions_to_process)} messages={n_new_messages} — no successful transcriptions to topic-model.")
        return

    from sentence_transformers import SentenceTransformer
    embedding_model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    topic_model = None if FORCE_REFIT else load_persisted_topic_model(embedding_model)

    topics_df = None
    if topic_model is None:
        assignments_pdf, topics_pdf, topic_model = fit_topics_fresh(docs_pdf, embedding_model)
        save_topic_model(topic_model)
        if not topics_pdf.empty:
            topics_df = spark.createDataFrame(topics_pdf, schema=TOPICS_SCHEMA)
    else:
        assignments_pdf = transform_topics(topic_model, docs_pdf)

    if not assignments_pdf.empty:
        assignments_df = spark.createDataFrame(assignments_pdf, schema=ASSIGNMENTS_SCHEMA)
        update_topic_assignments(spark, assignments_df, messages_table)

    if topics_df is not None:
        topics_df.writeTo(topics_table).using("iceberg").createOrReplace()

    print(
        f"season={season_year} sessions_processed={len(sessions_to_process)} "
        f"messages={n_new_messages} topic_model={'fit' if topics_df is not None else 'reused'}"
    )


if __name__ == "__main__":
    main()
