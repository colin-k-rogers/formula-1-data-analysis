"""GCP Managed Service for Apache Spark batch: transcribe F1 team-radio audio
with Whisper and topic-model the transcripts with BERTopic.

This is NOT a MotherDuck Flight — it needs a real Spark runtime plus GPU/CPU
time and model weights that don't fit MotherDuck's lightweight Flight runner.
Submit it as a Managed Spark Serverless batch (see README.md in this
directory for the one-time GCP setup and the exact `gcloud dataproc batches
submit pyspark` command). Output lands as two Iceberg tables in the
Lakehouse/BigLake Iceberg REST catalog, which MotherDuck attaches as the
`radio_lakehouse` database (see warehouse/setup_radio_lakehouse.sql) — dbt
reads them directly from there, no copy step needed.

Idempotent: each run overwrites the rows for the session_keys it processed
(see write_iceberg_tables), so re-running after OpenF1 corrects a session's
data does not duplicate rows.
"""
import io
import os
import time
from datetime import datetime

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


def fetch_team_radio(season_year):
    """Team-radio metadata (recording_url + who/when) for every Race session
    in `season_year`, following the same session-scoping as ingest_openf1."""
    all_race_type_sessions = fetch("sessions", {"year": season_year, "session_type": "Race"})
    sessions = [s for s in all_race_type_sessions if s.get("session_name") == "Race"]
    if not sessions:
        return []

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


def fit_topics(transcripts_pdf):
    """Fits BERTopic once, globally, over every transcript in the season so
    topic ids/labels are consistent across races (fitting per-partition
    would give each partition its own incompatible topic space)."""
    from bertopic import BERTopic
    from sentence_transformers import SentenceTransformer

    docs_df = transcripts_pdf[transcripts_pdf["transcript_text"].notna()].reset_index(drop=True)
    if docs_df.empty:
        empty_assignments = pd.DataFrame(columns=["radio_message_id", "topic_id", "topic_probability"])
        empty_topics = pd.DataFrame(columns=["topic_id", "label", "top_keywords", "doc_count"])
        return empty_assignments, empty_topics

    # calculate_probabilities=True costs more compute, but a season's worth
    # of radio clips is small (hundreds, not millions) and skipping it risks
    # an all-null topic_probability column that Spark would infer as
    # NullType — a type Iceberg can't reliably persist.
    embedding_model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    topic_model = BERTopic(embedding_model=embedding_model, calculate_probabilities=True)
    topics, probabilities = topic_model.fit_transform(docs_df["transcript_text"].tolist())

    assignments = pd.DataFrame({
        "radio_message_id": docs_df["radio_message_id"],
        "topic_id": topics,
        "topic_probability": probabilities,
    })

    topic_info = topic_model.get_topic_info()  # columns: Topic, Count, Name, Representation, ...
    topics_out = pd.DataFrame({
        "topic_id": topic_info["Topic"],
        "label": topic_info["Name"],
        "top_keywords": topic_info["Representation"].apply(lambda kws: ", ".join(kws[:8])),
        "doc_count": topic_info["Count"],
    })
    return assignments, topics_out


def write_iceberg_tables(spark, messages_df, topics_df, session_keys):
    """Overwrite rows for the session_keys processed this run (idempotent,
    same intent as the delete+insert upsert in ingest_openf1.load_table),
    then replace the topic dictionary wholesale since it's refit every run."""
    messages_table = f"{CATALOG_NAME}.{ICEBERG_NAMESPACE}.radio_messages"
    topics_table = f"{CATALOG_NAME}.{ICEBERG_NAMESPACE}.radio_topics"

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.{ICEBERG_NAMESPACE}")

    messages_df.createOrReplaceTempView("new_radio_messages")
    if spark.catalog.tableExists(messages_table):
        session_key_list = ", ".join(str(k) for k in session_keys)
        spark.sql(f"DELETE FROM {messages_table} WHERE session_key IN ({session_key_list})")
        spark.sql(f"INSERT INTO {messages_table} SELECT * FROM new_radio_messages")
    else:
        messages_df.writeTo(messages_table).using("iceberg").create()

    if spark.catalog.tableExists(topics_table):
        spark.sql(f"DROP TABLE {topics_table}")
    topics_df.writeTo(topics_table).using("iceberg").create()


def main():
    season_year = os.environ.get("SEASON_YEAR", "2025")

    spark = SparkSession.builder.appName("f1-radio-topic-modeling").getOrCreate()

    radio_metadata = fetch_team_radio(season_year)
    if not radio_metadata:
        print(f"No team radio found for season {season_year}; nothing to do.")
        return

    for m in radio_metadata:
        m["radio_message_id"] = f"{m['session_key']}:{m['driver_number']}:{m['date']}"
        # OpenF1 dates are ISO 8601 strings; Spark's TimestampType schema
        # needs actual datetime objects, not strings, to convert cleanly.
        m["message_date"] = datetime.fromisoformat(m.pop("date").replace("Z", "+00:00"))

    session_keys = sorted({m["session_key"] for m in radio_metadata})

    metadata_rows = [
        {k: m.get(k) for k in RADIO_METADATA_SCHEMA.fieldNames()} for m in radio_metadata
    ]
    metadata_df = spark.createDataFrame(metadata_rows, schema=RADIO_METADATA_SCHEMA)

    # Repartition so each partition gets a manageable, roughly even batch of
    # clips to download+transcribe; too many partitions wastes model-load
    # time, too few serializes the season onto a handful of executors.
    num_partitions = max(1, min(32, len(metadata_rows) // 10 or 1))
    transcribed_rdd = metadata_df.repartition(num_partitions).rdd.mapPartitions(transcribe_partition)
    transcripts_df = spark.createDataFrame(transcribed_rdd, schema=TRANSCRIPT_SCHEMA)
    transcripts_df.cache()

    transcripts_pdf = transcripts_df.toPandas()
    assignments_pdf, topics_pdf = fit_topics(transcripts_pdf)

    assignments_df = spark.createDataFrame(assignments_pdf) if not assignments_pdf.empty else None
    topics_df = spark.createDataFrame(topics_pdf) if not topics_pdf.empty else spark.createDataFrame(
        [], schema="topic_id int, label string, top_keywords string, doc_count long"
    )

    if assignments_df is not None:
        messages_df = transcripts_df.join(assignments_df, on="radio_message_id", how="left")
    else:
        messages_df = transcripts_df.withColumn("topic_id", F.lit(None).cast(IntegerType())).withColumn(
            "topic_probability", F.lit(None).cast(DoubleType())
        )

    write_iceberg_tables(spark, messages_df, topics_df, session_keys)

    print(
        f"season={season_year} radio_messages={messages_df.count()} "
        f"topics={topics_df.count()} sessions={len(session_keys)}"
    )


if __name__ == "__main__":
    main()
