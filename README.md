# f1 data analysis with Motherduck's Flights & Dives

## Purpose
Explore the capabilities of Motherduck's flights and dives tooling as well as indulge my interest in F1!

## What do you get?
An interactive dashboard or ["dive"](https://app.motherduck.com/dives) that allows you to look at how driver's lap times compared to each other over the course of a season. 

<img width="889" height="828" alt="Screenshot 2026-07-29 at 3 22 50 PM" src="https://github.com/user-attachments/assets/2353af94-9dcd-4860-933a-4b1183351a48" />

## How does it work?
Data is collected from the [OpenF1 API](https://openf1.org/) in a Flight then transformed using dbt Core.

The pipeline is two [MotherDuck Flights](https://motherduck.com/docs/concepts/flights/) — scheduled Python programs that run in the MotherDuck cloud — chained together, followed by a Dive for visualization:

1. **`f1-ingest-openf1`** ([flights/ingest_openf1](flights/ingest_openf1)) runs every Tuesday at 06:00 UTC. It pulls the season's Race sessions, meetings, drivers, and laps from the OpenF1 API and loads them into `f1.raw.*` tables. Each run deletes and re-inserts rows for the sessions it fetched, so it's idempotent and picks up corrections OpenF1 publishes after the fact.
2. **`f1-transform-dbt`** ([flights/transform_dbt](flights/transform_dbt)) runs 30 minutes later, at 06:30 UTC, so it always sees that day's ingested data. It executes `dbt build` against `md:f1` using the [dbt](dbt) project in this repo, which stages the raw OpenF1 tables and builds a `fct_lap_pace` mart: each driver's lap time compared to the fastest and median lap turned on that same lap of that same session. At build time, this Flight downloads the `dbt/` project straight from GitHub (a tarball of the `main` branch), so edits under `dbt/` take effect on the next run without needing to regenerate or push the Flight's source.
3. The [Dive](https://motherduck.com/docs/category/dives/) ([dives/season_pace_dive.tsx](dives/season_pace_dive.tsx)) is a React data app that queries `fct_lap_pace` live to render the driver-comparison chart shown above. [.dive-preview](.dive-preview) is a local Vite app (see `.claude/launch.json`) for previewing Dive changes before pushing them.
4. **`f1-race-pace-email`** ([flights/race_pace_email](flights/race_pace_email)) is an on-demand Flight that emails the same race-detail data and metrics shown in the Dive — race header, driver avg lap time / cumulative gap-to-fastest table, and a top-5 gap-to-fastest chart — for the most recent Race session. It sends via Gmail SMTP and requires a MotherDuck Flight secret named `gmail_smtp` (params `SMTP_USERNAME`, `SMTP_PASSWORD`).

Flights can be managed via SQL or MCP — see the [Flights SQL reference](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/flights/) and [Dives SQL reference](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/dives/) for details.

## Radio topic modeling

A second, heavier pipeline answers "what do drivers and teams talk about on
team radio, and how does that change over the season?" It transcribes team
radio with Whisper and topic-models the transcripts with BERTopic — both far
too heavy (model weights, CPU/GPU-bound inference) to run inside a MotherDuck
Flight — so this stage runs outside MotherDuck, on demand, rather than on a
Flight schedule:

1. **[spark_jobs/radio_topic_modeling](spark_jobs/radio_topic_modeling)** is a PySpark job submitted as an [AWS EMR Serverless](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/) job run (see its README for the full from-scratch AWS setup and the `aws emr-serverless start-job-run` command). It's incremental: each run only pulls and transcribes team-radio metadata from OpenF1 for sessions it hasn't already processed (with a local `faster-whisper` model distributed across executors), assigns topics into a BERTopic model persisted between runs so topic ids/labels stay stable race over race, and writes the results as two Iceberg tables registered in the AWS Glue Data Catalog (S3-backed).
2. **[warehouse/setup_radio_lakehouse.sql](warehouse/setup_radio_lakehouse.sql)** is a one-time (not scheduled) script that attaches that Glue catalog into MotherDuck as the `radio_lakehouse` database via `CREATE SECRET` + `CREATE DATABASE ... TYPE ICEBERG ENDPOINT_TYPE 'glue'`. Once run, the attachment persists across every session and Flight — no copy-into-`f1.raw` step is needed.
3. The same **`f1-transform-dbt`** Flight and [dbt](dbt) project stage `radio_lakehouse.raw.*` (see `models/sources.yml`) into `dim_radio_topics`, `fct_radio_messages` (each radio call joined to its driver, session, and in-progress lap), and `fct_driver_topic_race` (driver × session × topic message counts — the grain the Dive below charts).
4. **[dives/radio_topics_dive.tsx](dives/radio_topics_dive.tsx)** is a Dive with three views: season evolution (stacked chart of a driver's or team's topic mix race-by-race), topic over season (stacked chart of one topic's volume race-by-race, broken down by team or driver), and race detail (topic breakdown plus the underlying radio messages for one session). As with `season_pace_dive.tsx`, only one Dive source can be live in `.dive-preview/src/dive.tsx` at a time — copy in whichever one you're previewing.

### Running the whole radio pipeline in one go

Steps 1 and 3 above are two different systems on two different triggers —
`./run.sh` at a terminal for the Spark job, then the weekly `f1-transform-dbt`
Flight (or a manual run of it) for the dbt side — which means picking up a race
weekend's radio takes a human watching the first finish before starting the
second.

**`f1-radio-topics-pipeline`** ([flights/radio_topics_pipeline](flights/radio_topics_pipeline))
is an on-demand Flight that does all of it in one run: it submits the EMR
Serverless job run and blocks until it reaches `SUCCESS` (aborting before dbt if
it fails, so marts never get built on a half-written Iceberg table), then runs
`dbt build` selecting only the radio lineage — `stg_radio__messages+
stg_radio__topics+ topic_name_overrides+`, i.e. everything downstream of what
the Spark job just wrote, leaving the OpenF1 models to their own Tuesday
schedule — and finally reports the session and message counts now in
`fct_driver_topic_race`. There's nothing to "refresh" on the Dive itself: Dives
query live data on every render, so it's current the moment dbt commits; that
last step exists to prove a green run actually put new radio on the dashboard.

It needs two things the other Flights don't:

- **A `aws_emr` Flight secret** (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
  for an IAM user that can submit job runs.
  [warehouse/setup_flight_submitter_iam_user.sh](warehouse/setup_flight_submitter_iam_user.sh)
  creates that user, scoped to `StartJobRun`/`GetJobRun` on this one application
  plus `PassRole` of the job runtime role to EMR Serverless and nothing else.
  It's separate from the read-only Glue/S3 user in `setup_iam_user.sh`, which
  deliberately can't start anything.
- **Config matching your `.env`/`state.sh`**: `EMR_APPLICATION_ID` (required),
  plus `AWS_REGION`, `BUCKET_NAME`, `CATALOG_NAME`, `SEASON_YEAR`, and
  `WHISPER_MODEL_SIZE`. `EMR_EXECUTION_ROLE_ARN` is derived from the caller's own
  account via STS unless you set it.

`run.sh`'s three escape hatches work here too, as one-off run config overrides
rather than stored config: `FORCE_REFIT`, `REPROCESS_SESSIONS`, `REPROCESS_ALL`.
The spark-submit parameters the Flight builds mirror `run.sh`'s — change one,
change the other. `run.sh` itself stays the right tool for iterating on the
Spark job alone.
