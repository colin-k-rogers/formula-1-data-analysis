-- One-time setup: attach the GCP Lakehouse/BigLake Iceberg REST catalog
-- (populated by spark_jobs/radio_topic_modeling/job.py) into MotherDuck as
-- the `radio_lakehouse` database.
--
-- This is NOT a Flight and does not need to be re-run on a schedule: once
-- created, both the secret and the database persist in your MotherDuck
-- workspace and are visible from every session and Flight (including the
-- f1-transform-dbt Flight, whose dbt/models/sources.yml references
-- radio_lakehouse.raw.radio_messages / radio_topics directly — there's no
-- copy-into-f1.raw step).
--
-- Run this yourself after completing the GCP setup in
-- spark_jobs/radio_topic_modeling/README.md, with the placeholders below
-- filled in from that setup. Do not commit real credential values here —
-- keep this file with placeholders in git and paste real values only when
-- running it against your MotherDuck workspace.

CREATE SECRET radio_lakehouse_secret IN MOTHERDUCK (
    TYPE ICEBERG,
    -- From the GCP setup: a token or OAuth2 client that can call
    -- https://biglake.googleapis.com/iceberg/v1/restcatalog. The simplest
    -- option is a short-lived `gcloud auth print-access-token` value for
    -- testing; for anything long-running, mint a dedicated OAuth2 client
    -- instead so this doesn't need to be refreshed by hand.
    TOKEN '<GCP_ACCESS_TOKEN_OR_OAUTH_TOKEN>',
    EXTRA_HTTP_HEADERS MAP {
        'x-goog-user-project': '<PROJECT_ID>',
        'X-Iceberg-Access-Delegation': 'vended-credentials'
    }
);

CREATE DATABASE radio_lakehouse (
    TYPE ICEBERG,
    SECRET radio_lakehouse_secret,
    ENDPOINT 'https://biglake.googleapis.com/iceberg/v1/restcatalog',
    WAREHOUSE 'gs://<BUCKET_NAME>',
    DEFAULT_SCHEMA 'raw',
    READ_ONLY true
);

-- Sanity check once both statements above have run:
-- SELECT * FROM radio_lakehouse.raw.radio_messages LIMIT 10;
-- SELECT * FROM radio_lakehouse.raw.radio_topics;
