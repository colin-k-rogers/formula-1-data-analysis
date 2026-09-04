"""MotherDuck Flight: run the whole radio-topics pipeline end to end.

The radio-topics stack is three stages that have to happen in order, and
until now only the middle one lived in MotherDuck. This Flight chains all
three so a race weekend's radio can be picked up with a single run:

1. Submit spark_jobs/radio_topic_modeling as an AWS EMR Serverless job run
   (Whisper transcription + BERTopic, writing Iceberg tables into Glue) and
   block until it reaches SUCCESS. A FAILED/CANCELLED job run, or one that
   outlives POLL_TIMEOUT_SEC, aborts the Flight before dbt runs -- the
   whole point of waiting is to not build marts on half-written data.
2. `dbt build` only the radio lineage (see DBT_SELECT). The openf1 side of
   the project (dim_drivers / dim_sessions / fct_laps, which
   fct_radio_messages joins to) is left alone: it's owned by the
   f1-ingest-openf1 + f1-transform-dbt pair on their own Tuesday schedule,
   and rebuilding it here would do work this pipeline didn't invalidate.
3. Report what the Dive will now see. Dives query live data -- there is no
   dive-level refresh or cache to invalidate from outside the Dive -- so
   the honest final step is verifying the marts
   dives/radio_topics_dive.tsx reads actually moved, and logging the
   sessions/messages now in them.

Config (all non-secret, set on the Flight; every one has a default except
EMR_APPLICATION_ID):

  EMR_APPLICATION_ID     EMR Serverless application id (state.sh's APP_ID)
  AWS_REGION             region the application lives in
  EMR_EXECUTION_ROLE_ARN job runtime role; derived from the caller's own
                         account via STS when left empty
  BUCKET_NAME            S3 bucket holding the warehouse, logs, and model
  CATALOG_NAME           Iceberg catalog name (must match job.py)
  SEASON_YEAR            season to pull team radio for
  WHISPER_MODEL_SIZE     must match the size baked into the container image
  FORCE_REFIT            } the three escape hatches run.sh exposes; pass
  REPROCESS_SESSIONS     } them as one-off run config overrides rather
  REPROCESS_ALL          } than storing them on the Flight
  POLL_INTERVAL_SEC      how often to poll the job run
  POLL_TIMEOUT_SEC       give up waiting on the job run after this long
  DBT_SELECT             dbt selector for stage 2
  GITHUB_REF             repo ref to fetch the dbt project from

Secret `aws_emr` supplies AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY for an
IAM user that can StartJobRun/GetJobRun on the application and PassRole the
job runtime role -- see warehouse/setup_flight_submitter_iam_user.sh. This
is deliberately *not* the radio_lakehouse_secret IAM user, which is
read-only Glue/S3 and can't submit anything.

The spark-submit parameters below mirror
spark_jobs/radio_topic_modeling/run.sh. Change one, change the other.
"""
import io
import os
import pathlib
import subprocess
import tarfile
import time
import urllib.request

import boto3
import duckdb

GITHUB_REPO = "colin-k-rogers/formula-1-data-analysis"
FETCH_TIMEOUT_SEC = 30

PROJECT_DIR = pathlib.Path("/tmp/dbt_project")
SKIP_DIRS = {"target", "dbt_packages", "logs"}

# Everything downstream of the two Iceberg sources the Spark job writes,
# plus the seed dim_radio_topics overlays onto them. Expands to:
# stg_radio__messages, stg_radio__topics, topic_name_overrides,
# dim_radio_topics, fct_radio_messages, fct_driver_topic_race -- and the
# tests attached to them.
DEFAULT_DBT_SELECT = "stg_radio__messages+ stg_radio__topics+ topic_name_overrides+"

ENTRY_POINT = "local:///opt/radio_topic_modeling/job.py"
JOB_RUN_TERMINAL_STATES = {"SUCCESS", "FAILED", "CANCELLED"}

MD_DATABASE = "f1"
DIVE_MART = f'"{MD_DATABASE}"."marts"."fct_driver_topic_race"'


def log(message):
    print(message, flush=True)


def config(name, default=None):
    value = os.environ.get(name, "").strip()
    return value or default


def submit_job_run(emr, application_id, role_arn):
    """Start the EMR Serverless job run and return its id.

    Keep the --conf list here in the same order as run.sh's so the two stay
    diffable by eye.
    """
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
    # The same three opt-in knobs run.sh forwards, and for the same reason:
    # they're per-invocation decisions, so they only appear on the submit
    # when actually set (here, as a one-off run config override).
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
        # Unlike run.sh's unnamed submissions, tag these so a run started by
        # this Flight is identifiable in the EMR Serverless console.
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
    # Default deliberately sits under the Flight's own max_runtime_sec
    # (5400s) so a slow job run surfaces the message below instead of the
    # Flight being killed mid-poll with nothing useful in the log.
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
            # Deliberately not cancelling the job run -- it may well still
            # succeed, and a half-cancelled Spark write is worse than one
            # this Flight simply stopped watching.
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
        # A bare KeyError here reads like a bug rather than what it is:
        # the Flight is missing its secret reference entirely.
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
        # setup.sh always names the role this, and it lives in whichever
        # account these credentials belong to -- so ask STS rather than
        # making the operator copy an account id into config.
        account_id = session.client("sts").get_caller_identity()["Account"]
        role_arn = f"arn:aws:iam::{account_id}:role/f1-radio-topics-emrs-role"

    emr = session.client("emr-serverless")
    log(f"[1/3] Submitting EMR Serverless job run on {application_id} ({region})")
    job_run_id = submit_job_run(emr, application_id, role_arn)
    log(f"  started job run {job_run_id}; waiting for it to finish")
    wait_for_job_run(emr, application_id, job_run_id)
    log(f"  job run {job_run_id} SUCCEEDED")


def fetch_dbt_project():
    """Copy of f1-transform-dbt's fetch: build whatever is on GitHub."""
    ref = config("GITHUB_REF", "main")
    url = f"https://codeload.github.com/{GITHUB_REPO}/tar.gz/{ref}"
    with urllib.request.urlopen(url, timeout=FETCH_TIMEOUT_SEC) as resp:
        archive = resp.read()

    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        for member in tar.getmembers():
            if not member.isfile():
                continue
            # Strip the leading "<repo>-<ref>/" component GitHub adds, then
            # the "dbt/" prefix -- we only want that subdirectory.
            parts = pathlib.PurePosixPath(member.name).parts[1:]
            if len(parts) < 2 or parts[0] != "dbt":
                continue
            rel_parts = parts[1:]
            if any(part in SKIP_DIRS for part in rel_parts):
                continue

            dest = PROJECT_DIR / pathlib.PurePosixPath(*rel_parts)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(tar.extractfile(member).read())


def run_dbt():
    selector = config("DBT_SELECT", DEFAULT_DBT_SELECT)
    log(f"[2/3] dbt build --select {selector}")
    fetch_dbt_project()
    subprocess.run(
        [
            "dbt",
            "build",
            "--project-dir",
            str(PROJECT_DIR),
            "--profiles-dir",
            str(PROJECT_DIR),
            "--select",
            *selector.split(),
        ],
        check=True,
    )


def report_dive_data():
    """Confirm the marts dives/radio_topics_dive.tsx reads actually moved.

    There's no external refresh for a Dive -- it queries live data on every
    render, so the moment dbt commits, the Dive is current. What's worth
    doing instead is proving the run produced something the Dive can show,
    so a green Flight run means "new radio is on the dashboard" rather than
    just "no step raised".
    """
    log("[3/3] Verifying the Dive's marts")
    con = duckdb.connect("md:")
    total_sessions, total_messages = con.execute(
        f"SELECT count(DISTINCT session_key), sum(message_count) FROM {DIVE_MART}"
    ).fetchone()
    if not total_sessions:
        raise RuntimeError(
            f"{DIVE_MART} is empty after a successful dbt build -- the Dive "
            "would render nothing. Check the Spark job's Iceberg output and "
            "the radio_lakehouse attachment."
        )

    log(f"  {DIVE_MART}: {total_sessions} sessions, {total_messages} messages")
    log("  most recent sessions the Dive can now chart:")
    recent = con.execute(
        f"""
        SELECT session_date, country_name, session_name, sum(message_count)
        FROM {DIVE_MART}
        GROUP BY ALL
        ORDER BY session_date DESC
        LIMIT 5
        """
    ).fetchall()
    for session_date, country, session_name, messages in recent:
        log(f"    {session_date}  {country} {session_name}  {messages} messages")
    con.close()


def main():
    run_spark_job()
    run_dbt()
    report_dive_data()
    log("Radio-topics pipeline complete.")


if __name__ == "__main__":
    main()
