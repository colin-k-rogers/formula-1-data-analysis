"""MotherDuck Flight: run the whole radio-topics pipeline end to end.

Submits the EMR Serverless job run (spark_jobs/radio_topic_modeling), waits
for it to succeed, builds the radio dbt lineage, then reports what the Dive
can now show.

README.md's "Running the whole radio pipeline in one go" covers the setup,
the config keys, the `aws_emr` secret, and why each stage is scoped the way
it is. The spark-submit parameters below mirror run.sh's.
"""
# Run-visualization chart; node ids join to the @flight-run lines below. The
# convention is MotherDuck's flight viz guide (get_flight_viz_guide).
#
# @flight
# flowchart TD
#   submit[Submit EMR Serverless job run] --> wait[Wait for job run]:::wait
#   wait --> fetch[Fetch dbt project from GitHub]:::extract
#   fetch --> build[dbt build: radio lineage]:::query
#   build --> verify[Verify the Dive's marts]:::check
#   class build fanout
# @end-flight
import io
import json
import os
import pathlib
import re
import subprocess
import sys
import tarfile
import time
import urllib.request
from datetime import datetime, timezone

import boto3
import duckdb

GITHUB_REPO = "colin-k-rogers/formula-1-data-analysis"
FETCH_TIMEOUT_SEC = 30

PROJECT_DIR = pathlib.Path("/tmp/dbt_project")
SKIP_DIRS = {"target", "dbt_packages", "logs"}

# Expands to both stg_radio__* views, the seed, dim_radio_topics,
# fct_radio_messages, fct_driver_topic_race, and their tests.
DEFAULT_DBT_SELECT = "stg_radio__messages+ stg_radio__topics+ topic_name_overrides+"

ENTRY_POINT = "local:///opt/radio_topic_modeling/job.py"
JOB_RUN_TERMINAL_STATES = {"SUCCESS", "FAILED", "CANCELLED"}

MD_DATABASE = "f1"

# dbt's per-node progress line, e.g. "1 of 40 OK created sql view model
# main.stg_x ..... [OK in 0.12s]" or "3 of 40 FAIL 2 unique_x ..... [FAIL 2 in
# 0.1s]". The node's name is the last word before the dot leader.
DBT_PROGRESS = re.compile(
    r"\b\d+ of \d+ (START|OK|PASS|WARN|ERROR|FAIL|SKIP)\b(.*?)\s+\.*\s*\["
)
DBT_STATUS = {
    "START": "running",
    "OK": "ok",
    "PASS": "ok",
    "WARN": "ok",
    "ERROR": "failed",
    "FAIL": "failed",
    "SKIP": "skipped",
}

# Node last marked running, so a crash can be pinned on the step it hit.
_current = None


def emit(node, status, **fields):
    """Print one @flight-run progress line for the run-visualization chart."""
    global _current
    _current = (node, fields.get("key")) if status == "running" else None
    fields["ts"] = datetime.now(timezone.utc).isoformat()
    parts = [f"@flight-run {node} {status}"]
    for name, value in fields.items():
        value = " ".join(str(value).split())  # one event per line
        if " " in value or '"' in value or "=" in value:
            value = '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'
        parts.append(f"{name}={value}")
    print(" ".join(parts), flush=True)


def run_dbt(args, node):
    """Run dbt, passing its output through and reporting each model, seed and
    test it builds as one instance of `node`. Raises CalledProcessError on
    failure."""
    proc = subprocess.Popen(
        ["dbt", *args], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    reported = 0
    for line in proc.stdout:
        sys.stdout.write(line)
        sys.stdout.flush()
        m = DBT_PROGRESS.search(line)
        if not m:
            continue
        verb, rest = m.groups()
        name = rest.split()[-1]
        fields = {"key": name}
        # "OK created ... model main.x" / "OK loaded seed file main.x" persist
        # a relation; a test's PASS doesn't.
        if verb == "OK":
            fields["effect"] = f"table:f1.{name}"
        emit(node, DBT_STATUS[verb], **fields)
        reported += 1
    if proc.wait() != 0:
        # Per-node failures are already on their own rows; this covers dbt
        # dying before it built anything (bad profile, parse error).
        if not reported:
            emit(node, "failed", error=f"dbt exited {proc.returncode}")
        raise subprocess.CalledProcessError(proc.returncode, ["dbt", *args])


def log(message):
    print(message, flush=True)


def config(name, default=None):
    value = os.environ.get(name, "").strip()
    return value or default


def dive_mart():
    """fct_driver_topic_race's relation, in whatever marts schema this run's
    SCHEMA_SUFFIX resolves to (branch-scoped in preview, unsuffixed in prod)."""
    schema = f"marts{config('SCHEMA_SUFFIX', '')}"
    return f'"{MD_DATABASE}"."{schema}"."fct_driver_topic_race"'


def submit_job_run(emr, application_id, role_arn):
    """Start the EMR Serverless job run and return its id."""
    bucket = config("BUCKET_NAME", "f1-radio-topics-lakehouse")
    catalog = config("CATALOG_NAME", "radio")
    season_year = config("SEASON_YEAR", "2026")
    whisper_model_size = config("WHISPER_MODEL_SIZE", "medium")
    model_store_path = f"s3://{bucket}/models/bertopic/model.tar.gz"

    params = [
        "--conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python",
        "--conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=/opt/venv/bin/python",
        "--conf spark.executorEnv.PYSPARK_PYTHON=/opt/venv/bin/python",
        f"--conf spark.emr-serverless.driverEnv.SEASON_YEAR={season_year}",
        f"--conf spark.emr-serverless.driverEnv.MODEL_STORE_PATH={model_store_path}",
        f"--conf spark.executorEnv.WHISPER_MODEL_SIZE={whisper_model_size}",
    ]
    # Per-invocation knobs: only sent when actually set.
    for knob in ("FORCE_REFIT", "REPROCESS_SESSIONS", "REPROCESS_ALL"):
        value = config(knob)
        if value:
            log(f"  {knob}={value}")
            params.append(f"--conf spark.emr-serverless.driverEnv.{knob}={value}")
    params += [
        "--conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar",
        "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        f"--conf spark.sql.defaultCatalog={catalog}",
        f"--conf spark.sql.catalog.{catalog}=org.apache.iceberg.spark.SparkCatalog",
        f"--conf spark.sql.catalog.{catalog}.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
        f"--conf spark.sql.catalog.{catalog}.warehouse=s3://{bucket}/warehouse",
        f"--conf spark.sql.catalog.{catalog}.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
    ]

    response = emr.start_job_run(
        applicationId=application_id,
        executionRoleArn=role_arn,
        # Identifies Flight-started runs in the EMR Serverless console.
        name=f"f1-radio-topics-flight-{os.environ.get('MOTHERDUCK_FLIGHTS_RUN', 'adhoc')}",
        jobDriver={
            "sparkSubmit": {
                "entryPoint": ENTRY_POINT,
                "sparkSubmitParameters": " ".join(params),
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": f"s3://{bucket}/logs/"}
            }
        },
    )
    return response["jobRunId"]


def wait_for_job_run(emr, application_id, job_run_id):
    """Block until the job run is terminal; raise unless it SUCCESSed."""
    interval = int(config("POLL_INTERVAL_SEC", "30"))
    # Sits under the Flight's own max_runtime_sec (5400s), so a slow job run
    # raises below instead of the Flight being killed mid-poll.
    timeout = int(config("POLL_TIMEOUT_SEC", "4800"))
    deadline = time.monotonic() + timeout

    state = None
    while True:
        job_run = emr.get_job_run(
            applicationId=application_id, jobRunId=job_run_id
        )["jobRun"]
        if job_run["state"] != state:
            state = job_run["state"]
            log(f"  job run {job_run_id}: {state}")
        if state in JOB_RUN_TERMINAL_STATES:
            break
        if time.monotonic() >= deadline:
            # Left running on purpose: a half-cancelled Spark write is
            # worse than one this Flight merely stopped watching.
            raise TimeoutError(
                f"Job run {job_run_id} still {state} after {timeout}s; it is "
                "still running in EMR Serverless. Check it with `aws "
                f"emr-serverless get-job-run --application-id {application_id} "
                f"--job-run-id {job_run_id}`, then re-run this Flight once "
                "it's done to pick up the dbt + verification stages."
            )
        time.sleep(interval)

    if state != "SUCCESS":
        details = job_run.get("stateDetails") or "(no stateDetails)"
        raise RuntimeError(
            f"Job run {job_run_id} ended {state}: {details}. Driver/executor "
            f"logs are under s3://{config('BUCKET_NAME', 'f1-radio-topics-lakehouse')}/logs/."
        )


def run_spark_job():
    emit("submit", "running")
    application_id = config("EMR_APPLICATION_ID")
    if not application_id:
        raise RuntimeError(
            "EMR_APPLICATION_ID is not set on this Flight's config. It's the "
            "APP_ID that spark_jobs/radio_topic_modeling/setup.sh wrote to "
            "state.sh."
        )
    region = config("AWS_REGION", "us-east-1")
    try:
        access_key_id = os.environ["aws_emr_AWS_ACCESS_KEY_ID"]
        secret_access_key = os.environ["aws_emr_AWS_SECRET_ACCESS_KEY"]
    except KeyError as missing:
        # A bare KeyError reads like a bug, not a missing secret reference.
        raise RuntimeError(
            f"{missing.args[0]} is not in the environment. This Flight needs "
            "the `aws_emr` secret (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) "
            "listed in its secret references -- see "
            "warehouse/setup_flight_submitter_iam_user.sh."
        ) from None
    session = boto3.session.Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name=region,
    )

    role_arn = config("EMR_EXECUTION_ROLE_ARN")
    if not role_arn:
        # Ask STS rather than making the operator copy an account id in.
        account_id = session.client("sts").get_caller_identity()["Account"]
        role_arn = f"arn:aws:iam::{account_id}:role/f1-radio-topics-emrs-role"

    emr = session.client("emr-serverless")
    log(f"[1/3] Submitting EMR Serverless job run on {application_id} ({region})")
    job_run_id = submit_job_run(emr, application_id, role_arn)
    emit(
        "submit", "ok", effect=f"net:emr-serverless.{region}.amazonaws.com"
    )
    log(f"  started job run {job_run_id}; waiting for it to finish")
    emit("wait", "running")
    wait_for_job_run(emr, application_id, job_run_id)
    emit("wait", "ok")
    log(f"  job run {job_run_id} SUCCEEDED")


def fetch_dbt_project():
    """Same fetch as f1-transform-dbt: build whatever is on GitHub."""
    ref = config("GITHUB_REF", "main")
    url = f"https://codeload.github.com/{GITHUB_REPO}/tar.gz/{ref}"
    written = 0
    with urllib.request.urlopen(url, timeout=FETCH_TIMEOUT_SEC) as resp:
        archive = resp.read()

    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        for member in tar.getmembers():
            if not member.isfile():
                continue
            # Strip GitHub's "<repo>-<ref>/" prefix, then "dbt/".
            parts = pathlib.PurePosixPath(member.name).parts[1:]
            if len(parts) < 2 or parts[0] != "dbt":
                continue
            rel_parts = parts[1:]
            if any(part in SKIP_DIRS for part in rel_parts):
                continue

            dest = PROJECT_DIR / pathlib.PurePosixPath(*rel_parts)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(tar.extractfile(member).read())
            written += 1
    return written


def build_radio_marts():
    selector = config("DBT_SELECT", DEFAULT_DBT_SELECT)
    log(f"[2/3] dbt build --select {selector}")
    emit("fetch", "running")
    files = fetch_dbt_project()
    emit("fetch", "ok", rows=files, effect="net:codeload.github.com")
    run_dbt(
        [
            "build",
            "--project-dir",
            str(PROJECT_DIR),
            "--profiles-dir",
            str(PROJECT_DIR),
            "--vars",
            json.dumps({"schema_suffix": config("SCHEMA_SUFFIX", "")}),
            "--select",
            *selector.split(),
        ],
        "build",
    )


def report_dive_data():
    """Log what the Dive can now show, and fail if it would show nothing."""
    log("[3/3] Verifying the Dive's marts")
    emit("verify", "running")
    con = duckdb.connect("md:")
    # No local timezone in the container, so DuckDB reports 'Etc/Unknown'
    # and pytz dies converting a TIMESTAMPTZ. Pin UTC, and format
    # session_date in SQL below, since this step only prints these values.
    con.execute("SET TimeZone = 'UTC'")
    total_sessions, total_messages = con.execute(
        f"SELECT count(DISTINCT session_key), sum(message_count) FROM {dive_mart()}"
    ).fetchone()
    if not total_sessions:
        raise RuntimeError(
            f"{dive_mart()} is empty after a successful dbt build -- the Dive "
            "would render nothing. Check the Spark job's Iceberg output and "
            "the radio_lakehouse attachment."
        )

    log(f"  {dive_mart()}: {total_sessions} sessions, {total_messages} messages")
    log("  most recent sessions the Dive can now chart:")
    recent = con.execute(
        f"""
        SELECT
            strftime(session_date, '%Y-%m-%d') AS session_day,
            country_name,
            session_name,
            sum(message_count)
        FROM {dive_mart()}
        GROUP BY ALL
        ORDER BY session_day DESC
        LIMIT 5
        """
    ).fetchall()
    for session_day, country, session_name, messages in recent:
        log(f"    {session_day}  {country} {session_name}  {messages} messages")
    con.close()
    emit("verify", "ok", rows=total_messages)


def main():
    run_spark_job()
    build_radio_marts()
    report_dive_data()
    log("Radio-topics pipeline complete.")


if __name__ == "__main__":
    try:
        main()
        emit("@flight", "ok", exit=0)
    except Exception as err:
        if _current:
            node, key = _current
            extra = {"key": key} if key else {}
            emit(node, "failed", error=f"{type(err).__name__}: {err}", **extra)
        emit("@flight", "failed", exit=1)
        raise
