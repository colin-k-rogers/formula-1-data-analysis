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
already in radio_messages (or are explicitly listed in REPROCESS_SESSIONS,
or REPROCESS_ALL=true is set to reprocess every session in SEASON_YEAR --
e.g. after switching WHISPER_MODEL_SIZE and wanting the whole season
re-transcribed with it), and assigns their topics with `BERTopic.transform()`
against a model
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
import hashlib
import io
import os
import re
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

# "small" still mis-hears enough short, jargon-heavy radio calls to matter
# for topic modeling (e.g. "Plan A" -> "plane") — "medium" is a further
# accuracy step up (~2x the compute of "small") that's still comfortably
# fast enough for a season's few thousand short clips. This default must
# match whatever WHISPER_MODEL_SIZE the image was built with (see
# Dockerfile's matching ARG and setup.sh's --build-arg) — it only controls
# what gets requested at runtime, not what's actually pre-baked.
WHISPER_MODEL_SIZE = os.environ.get("WHISPER_MODEL_SIZE", "medium")
# Biases Whisper toward F1 radio's actual vocabulary instead of the nearest
# everyday-English homophone (e.g. "Plan A"/"Plan B" -> "plane", "box" ->
# "box" mis-heard as something else) -- faster-whisper primes decoding with
# this as prior context rather than treating it as literal transcript.
WHISPER_INITIAL_PROMPT = (
    "Formula 1 team radio. Box box box, box this lap, pit stop, undercut, "
    "overcut, Plan A, Plan B, safety car, virtual safety car, DRS, push now, "
    "tyres, tires, degradation, undercut window, gap, delta, copy, understood, "
    "car ahead, car behind."
)
EMBEDDING_MODEL_NAME = os.environ.get("EMBEDDING_MODEL_NAME", "all-MiniLM-L6-v2")
# Driver/team names dominate short utterances enough that BERTopic clusters
# by WHO is mentioned instead of the shared theme -- stripped from the text
# fed to the topic model only (driver_number/team are already columns
# elsewhere, so no identity is lost). See build_name_strip_pattern() below.
MIN_NAME_STRIP_LEN = 4
# First/last names or team names that double as ordinary racing vocabulary
# (e.g. "push to the max") -- excluded so stripping names doesn't delete
# real topical content. Extend if a future grid introduces another collision.
AMBIGUOUS_NAME_STOPWORDS = {"max"}
CATALOG_NAME = os.environ.get("ICEBERG_CATALOG_NAME", "radio")
ICEBERG_NAMESPACE = os.environ.get("ICEBERG_NAMESPACE", "raw")
# Where the fitted BERTopic model is persisted between runs, e.g.
# s3://bucket/models/bertopic/model.tar.gz. Without this set, every run
# fits fresh (same as FORCE_REFIT=true) since there's nothing to load.
MODEL_STORE_PATH = os.environ.get("MODEL_STORE_PATH")
# Bump whenever a change alters what the persisted model was fit ON (e.g.
# anonymized vs. raw text) -- load_persisted_topic_model() treats a version
# mismatch the same as "nothing persisted yet" and fits fresh instead of
# transform()-ing new documents into a model built under different rules.
MODEL_SCHEMA_VERSION = "2"
FORCE_REFIT = os.environ.get("FORCE_REFIT", "false").lower() == "true"
REPROCESS_SESSIONS = {
    int(s) for s in os.environ.get("REPROCESS_SESSIONS", "").split(",") if s.strip()
}
# Reprocess every session in SEASON_YEAR, not just ones listed individually
# in REPROCESS_SESSIONS -- for a whole-season re-transcription (e.g. after
# switching WHISPER_MODEL_SIZE) without having to enumerate session keys.
REPROCESS_ALL = os.environ.get("REPROCESS_ALL", "false").lower() == "true"


def fetch(endpoint, params):
    """GET with retries; mirrors flights/ingest-openf1/main.py's fetch(). A
    404 means OpenF1 has no rows for these params (e.g. team radio for a
    session that never happened) — treat that as an empty result rather
    than an error to retry into the ground."""
    url = f"{BASE_URL}/{endpoint}"
    last_err = None
    retries_used = 0
    rate_limit_retries_used = 0
    while retries_used < MAX_RETRIES and rate_limit_retries_used < MAX_RATE_LIMIT_RETRIES:
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SEC)
            if resp.status_code == 404:
                return []
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


# OpenF1 `session_name` values whose team radio gets transcribed. Deliberately
# excludes practice sessions (low-signal, mostly engineering chatter) and
# "Sprint Qualifying"/"Sprint Shootout" (the short knockout session that sets
# the sprint grid, not what's usually meant by a weekend's "qualifying").
# Fetched without a `session_type` filter because Sprint shares its
# session_type ("Race") with the Race session itself -- session_name is the
# only field that actually distinguishes them.
TARGET_SESSION_NAMES = {"Race", "Qualifying", "Sprint"}


def fetch_target_sessions(season_year):
    """All Race, Qualifying, and Sprint sessions for `season_year` (metadata
    only — cheap, and safe to call every run since it's just used to
    discover which sessions exist, not to fetch their radio). Cancelled
    sessions have no radio data at all (same reason ingest_openf1 skips them
    for laps), so skip them too."""
    all_sessions = fetch("sessions", {"year": season_year})
    return [
        s
        for s in all_sessions
        if s.get("session_name") in TARGET_SESSION_NAMES and not s.get("is_cancelled")
    ]


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


def fetch_driver_rosters(session_keys):
    """Driver name/team metadata for the given session_keys, deduped by
    name_acronym -- feeds build_name_strip_pattern(), never stored."""
    rosters = []
    seen_acronyms = set()
    for session_key in session_keys:
        for d in fetch("drivers", {"session_key": session_key}):
            acronym = d.get("name_acronym")
            if acronym and acronym not in seen_acronyms:
                seen_acronyms.add(acronym)
                rosters.append(d)
        time.sleep(INTER_REQUEST_SLEEP_SEC)
    return rosters


def build_name_strip_pattern(driver_rosters):
    """Compiles a case-insensitive, word-boundary regex matching any driver
    first/last name or team name from `driver_rosters` (excluding short/
    ambiguous tokens -- see MIN_NAME_STRIP_LEN, AMBIGUOUS_NAME_STOPWORDS).
    Returns None if there's nothing to strip."""
    tokens = set()
    for d in driver_rosters:
        candidates = [d.get("first_name"), d.get("last_name"), d.get("team_name")]
        for token in candidates:
            if token and len(token) >= MIN_NAME_STRIP_LEN and token.lower() not in AMBIGUOUS_NAME_STOPWORDS:
                tokens.add(token)
    if not tokens:
        return None
    # Longest-first so a multi-word team name (e.g. "Red Bull Racing")
    # matches whole rather than leaving fragments behind piecemeal.
    alternation = "|".join(re.escape(t) for t in sorted(tokens, key=len, reverse=True))
    return re.compile(rf"\b(?:{alternation})\b", re.IGNORECASE)


def anonymize_names(text, name_strip_pattern):
    """Strips driver/team names out of `text` for topic-modeling purposes
    only (transcript_text itself is never touched). Falls back to the
    original text if stripping would leave nothing."""
    if name_strip_pattern is None or not text:
        return text
    stripped, n_subs = name_strip_pattern.subn("", text)
    if n_subs == 0:
        return text
    collapsed = re.sub(r"\s{2,}", " ", stripped).strip(" ,.")
    return collapsed or text


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
            # Force English rather than letting Whisper free-detect it: F1
            # radio is overwhelmingly English, and on short/noisy/near-silent
            # clips (routine for radio traffic) Whisper's language detector
            # can lock onto a random language and hallucinate fluent-looking
            # text in it — Welsh is a well-documented hallucination target
            # for quiet/noisy audio specifically. A wrong-but-plausible
            # transcript is worse for topic modeling than an English
            # transcript that's merely poor.
            # Some clips run minutes long with mostly dead air; without
            # vad_filter, Whisper decodes the silence as speech and loops on
            # the same hallucinated sentence. condition_on_previous_text=False
            # stops it feeding on its own prior output once that starts, and
            # costs nothing since each clip has no real cross-segment context.
            segments, info = model.transcribe(
                audio_buf, beam_size=5, language="en", initial_prompt=WHISPER_INITIAL_PROMPT,
                vad_filter=True, condition_on_previous_text=False,
            )
            text = " ".join(seg.text.strip() for seg in segments).strip()
            if not text:
                # vad_filter occasionally (nondeterministically -- a
                # numerical edge case in faster-whisper's feature extractor
                # on long, silent audio) decides a clip has no speech when it
                # does. Retry without VAD rather than lose the transcript --
                # worst case this reverts to the pre-VAD hallucination loop,
                # still better than nothing.
                segments, info = model.transcribe(
                    io.BytesIO(resp.content), beam_size=5, language="en",
                    initial_prompt=WHISPER_INITIAL_PROMPT, condition_on_previous_text=False,
                )
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
    # Split on the scheme separator rather than assuming "s3://" specifically
    # — EMR/Spark configs commonly write S3 paths as "s3a://" instead, which
    # a fixed-length prefix strip would parse into a garbage bucket/key.
    _, _, rest = uri.partition("://")
    bucket, _, key = rest.partition("/")
    return bucket, key


def _model_version_key(key):
    return f"{key}.version"


def load_persisted_topic_model(embedding_model):
    """Loads the BERTopic model a prior run saved to MODEL_STORE_PATH, so
    this run can assign new documents into that SAME topic space via
    transform() instead of fitting a new, incompatible one. Returns None if
    MODEL_STORE_PATH isn't configured, nothing's been saved yet, or what's
    saved was fit under a different MODEL_SCHEMA_VERSION (the caller treats
    all of these the same as "first run ever": fit fresh)."""
    if not MODEL_STORE_PATH:
        return None
    from bertopic import BERTopic

    bucket, key = _split_s3_uri(MODEL_STORE_PATH)
    s3 = boto3.client("s3")
    try:
        version_obj = s3.get_object(Bucket=bucket, Key=_model_version_key(key))
        if version_obj["Body"].read().decode() != MODEL_SCHEMA_VERSION:
            return None
    except botocore.exceptions.ClientError as err:
        if err.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
            return None
        raise

    with tempfile.TemporaryDirectory() as tmp:
        archive_path = os.path.join(tmp, "model.tar.gz")
        try:
            s3.download_file(bucket, key, archive_path)
        except botocore.exceptions.ClientError as err:
            if err.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
                return None
            raise
        extract_dir = os.path.join(tmp, "model")
        with tarfile.open(archive_path) as tar:
            tar.extractall(extract_dir)
        return BERTopic.load(extract_dir, embedding_model=embedding_model)


def save_topic_model(topic_model):
    """Persists a freshly fit BERTopic model (and its MODEL_SCHEMA_VERSION)
    to MODEL_STORE_PATH for a later run's load_persisted_topic_model() to
    pick up. No-op if MODEL_STORE_PATH isn't configured (every run just
    fits fresh instead)."""
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
        s3 = boto3.client("s3")
        s3.upload_file(archive_path, bucket, key)
        s3.put_object(Bucket=bucket, Key=_model_version_key(key), Body=MODEL_SCHEMA_VERSION.encode())


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


# Deliberately curated, not a general dedup pass: each group names the SAME
# radio call in different words, verified against real transcripts. Getting
# a group wrong silently conflates two different topics, so only add one
# after checking sample transcripts.
SYNONYM_MERGE_GROUPS = [
    ["box", "pit"],
]


def _topic_ids_matching_keyword(topic_info, keyword):
    """Ids of topics (excluding -1, the outlier bucket) whose top keywords
    include `keyword` as a whole word — lets merge_synonym_topics() locate
    topics by content instead of by id, since ids/counts shift on every
    refit. Matches `keyword` as a whole word WITHIN each (possibly
    multi-word) representation phrase, e.g. "pit" matches "pit stop" -- a
    plain `keyword in representation` membership check would miss that,
    since representation entries are rarely single bare words."""
    pattern = re.compile(rf"\b{re.escape(keyword)}\b")
    return [
        topic_id
        for topic_id, representation in zip(topic_info["Topic"], topic_info["Representation"])
        if topic_id != -1 and any(pattern.search(kw.lower()) for kw in representation)
    ]


def merge_synonym_topics(topic_model, docs):
    """Merges topics that nr_topics="auto" can't catch because they're
    lexically distinct despite describing the same call (see
    SYNONYM_MERGE_GROUPS) — mutates `topic_model` in place. `docs` must be
    the exact same list already passed to fit_transform(), same order."""
    for keywords in SYNONYM_MERGE_GROUPS:
        # Re-fetched every iteration since merging one group renumbers
        # topic ids, which would stale-out a topic_info computed earlier.
        topic_info = topic_model.get_topic_info()
        topic_ids = {
            topic_id
            for keyword in keywords
            for topic_id in _topic_ids_matching_keyword(topic_info, keyword)
        }
        if len(topic_ids) > 1:
            topic_model.merge_topics(docs, topics_to_merge=list(topic_ids))


def fit_topics_fresh(docs_pdf, embedding_model):
    """Fits a brand-new BERTopic model over `docs_pdf` -- used for the very
    first run ever and for a deliberate FORCE_REFIT. Clusters/labels are
    computed from model_text (anonymized copy of transcript_text -- see
    anonymize_names()). Returns (assignments, topics, fitted_model)."""
    from bertopic import BERTopic
    from bertopic.representation import KeyBERTInspired
    from sklearn.feature_extraction.text import CountVectorizer
    from umap import UMAP

    # Radio calls are short (a sentence or two) and there are only hundreds
    # of them a season — BERTopic's defaults are tuned for larger,
    # longer-form corpora and collapse data like this into one dominant
    # cluster. A smaller UMAP neighborhood lets more, smaller topics form
    # instead of one catch-all blob; min_topic_size below then controls how
    # many of those survive as their own topic vs. get folded into a
    # neighbor or the outlier bucket.
    umap_model = UMAP(n_neighbors=10, n_components=5, min_dist=0.0, metric="cosine", random_state=42)
    # Default c-TF-IDF keywords are plain word counts with no stopword
    # filtering, so labels end up as "the, to, it, we" instead of actual
    # racing vocabulary. Strip stopwords and allow 2-word phrases (e.g.
    # "pit stop", "tyre wear") so labels carry real information.
    vectorizer_model = CountVectorizer(stop_words="english", ngram_range=(1, 2), min_df=2)
    # Re-ranks each topic's keywords by embedding similarity to the topic
    # itself, reusing the same embedding_model already loaded (no extra
    # model or network call) — meaningfully more readable than raw
    # c-TF-IDF counts alone.
    representation_model = KeyBERTInspired()

    # Raised from an earlier 8: that produced 80+ topics on a multi-season
    # corpus, including a long tail of topics with only a dozen-ish
    # messages each (individual driver names, one-off events) that were too
    # granular to be useful for tracking how *broad* conversation themes
    # evolve across a season. A larger minimum folds those into the nearest
    # topic (or the outlier bucket) instead, leaving a smaller set of
    # topics with enough volume to actually be worth tracking race-to-race.
    topic_model = BERTopic(
        embedding_model=embedding_model,
        umap_model=umap_model,
        vectorizer_model=vectorizer_model,
        representation_model=representation_model,
        min_topic_size=25,
        # Auto-merges topics with similar c-TF-IDF vectors post-fit -- only
        # catches shared vocabulary, so lexically-distinct synonyms (see
        # merge_synonym_topics() below) still need a separate pass.
        nr_topics="auto",
        calculate_probabilities=True,
    )
    docs = docs_pdf["model_text"].tolist()
    topic_model.fit_transform(docs)
    merge_synonym_topics(topic_model, docs)

    topic_info = topic_model.get_topic_info()  # columns: Topic, Count, Name, Representation, ...
    # merge_synonym_topics (if it merged anything) updates topic_model's
    # topics_/probabilities_ in place (BERTopic's merge_topics sums the
    # probability columns of merged topics together) -- read both from
    # there, not the pre-merge `topics`/`probabilities` fit_transform
    # returned, so topic_id and topic_probability stay consistent.
    assignments = _assignments_frame(docs_pdf["radio_message_id"], topic_model.topics_, topic_model.probabilities_)
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
    stable across runs in the common (non-FORCE_REFIT) case. Uses
    model_text (name-anonymized), matching what the model was fit on."""
    topics, probabilities = topic_model.transform(docs_pdf["model_text"].tolist())
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
    season_year = os.environ.get("SEASON_YEAR", "2026")

    spark = SparkSession.builder.appName("f1-radio-topic-modeling").getOrCreate()

    messages_table = f"{CATALOG_NAME}.{ICEBERG_NAMESPACE}.radio_messages"
    topics_table = f"{CATALOG_NAME}.{ICEBERG_NAMESPACE}.radio_topics"
    table_existed_before_this_run = spark.catalog.tableExists(messages_table)

    # Only count a session as done once it has at least one *successful*
    # transcription — if every clip in a session failed (a transient
    # network blip, say), leave it eligible for a natural retry next run
    # instead of silently writing it off forever.
    already_processed = set()
    if table_existed_before_this_run:
        already_processed = {
            row["session_key"]
            for row in spark.sql(
                f"SELECT DISTINCT session_key FROM {messages_table} WHERE transcript_text IS NOT NULL"
            ).collect()
        }

    all_sessions = fetch_target_sessions(season_year)
    sessions_to_process = [
        s for s in all_sessions
        if REPROCESS_ALL
        or s["session_key"] not in already_processed
        or s["session_key"] in REPROCESS_SESSIONS
    ]
    radio_metadata = fetch_team_radio_for_sessions(sessions_to_process) if sessions_to_process else []

    # A run with nothing new to transcribe for this SEASON_YEAR still needs
    # to reach the topic-modeling stage below when FORCE_REFIT is set —
    # refitting is a deliberate action on the corpus that's already
    # persisted, not something that depends on this run finding new
    # sessions. Only bail out early when there's truly nothing to do at all.
    new_rows_df = None
    n_new_messages = 0
    if radio_metadata:
        for m in radio_metadata:
            # session_key:driver_number:date is descriptive but not
            # guaranteed unique on its own — two radio calls could share a
            # timestamp at OpenF1's reported precision. recording_url is
            # guaranteed unique (each clip is a distinct file), so fold a
            # short hash of it in too.
            url_hash = hashlib.sha1(m["recording_url"].encode()).hexdigest()[:8]
            m["radio_message_id"] = f"{m['session_key']}:{m['driver_number']}:{m['date']}:{url_hash}"
            # OpenF1 dates are ISO 8601 strings; Spark's TimestampType schema
            # needs actual datetime objects, not strings, to convert cleanly.
            m["message_date"] = datetime.fromisoformat(m.pop("date").replace("Z", "+00:00"))

        metadata_rows = [
            {k: m.get(k) for k in RADIO_METADATA_SCHEMA.fieldNames()} for m in radio_metadata
        ]
        metadata_df = spark.createDataFrame(metadata_rows, schema=RADIO_METADATA_SCHEMA)

        # Repartition so each partition gets a manageable, roughly even batch
        # of clips to download+transcribe; too many partitions wastes
        # model-load time, too few serializes the run onto a handful of
        # executors.
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

        # Land this run's transcripts first (topic assignment filled in
        # below). For a fresh fit this also makes them visible to the "read
        # the whole corpus back" query that follows.
        write_new_messages(spark, new_rows_df, messages_table)
    elif not FORCE_REFIT:
        print(f"season={season_year}: nothing new to transcribe this run; nothing to do.")
        return

    from sentence_transformers import SentenceTransformer
    embedding_model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    topic_model = None if FORCE_REFIT else load_persisted_topic_model(embedding_model)

    # Fitting fresh — whether from a deliberate FORCE_REFIT, no
    # MODEL_STORE_PATH configured, or nothing persisted yet — needs the
    # WHOLE corpus, not just this run's new rows, or radio_topics getting
    # wholesale-replaced below would strand every previously-assigned
    # message's topic_id with no matching row (shown as "Uncategorized").
    # Only a successfully loaded persisted model gets just this run's new
    # rows, since transform() assigns them into its existing topic space
    # without needing to see the rest of the corpus again.
    if topic_model is None:
        docs_pdf = spark.sql(
            f"SELECT radio_message_id, transcript_text FROM {messages_table} WHERE transcript_text IS NOT NULL"
        ).toPandas()
    else:
        docs_pdf = new_rows_df.filter("transcript_text IS NOT NULL").select("radio_message_id", "transcript_text").toPandas()

    if docs_pdf.empty:
        print(f"season={season_year} sessions_processed={len(sessions_to_process)} messages={n_new_messages} — no successful transcriptions to topic-model.")
        return

    if topic_model is None:
        # A fresh fit trains on the WHOLE historical corpus (potentially
        # several seasons), so the roster needs every session ever
        # ingested -- scoping it to just this run's season would leave
        # older seasons' driver names un-stripped in the same fit.
        roster_session_keys = {
            row["session_key"]
            for row in spark.sql(f"SELECT DISTINCT session_key FROM {messages_table}").collect()
        }
    else:
        # transform() only assigns this run's (re)transcribed sessions into
        # the existing topic space, so only their drivers need to be here.
        roster_session_keys = {s["session_key"] for s in sessions_to_process}
    driver_rosters = fetch_driver_rosters(roster_session_keys) if roster_session_keys else []
    name_strip_pattern = build_name_strip_pattern(driver_rosters)
    docs_pdf["model_text"] = docs_pdf["transcript_text"].map(lambda t: anonymize_names(t, name_strip_pattern))

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
