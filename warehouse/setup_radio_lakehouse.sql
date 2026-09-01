-- One-time setup: attach the AWS Glue Data Catalog (populated by
-- spark_jobs/radio_topic_modeling/job.py, backed by S3) into MotherDuck as
-- the `radio_lakehouse` database.
--
-- This is NOT a Flight and does not need to be re-run on a schedule: once
-- created, both the secret and the database persist in your MotherDuck
-- workspace and are visible from every session and Flight (including the
-- f1-transform-dbt Flight, whose dbt/models/sources.yml references
-- radio_lakehouse.raw.radio_messages / radio_topics directly — there's no
-- copy-into-f1.raw step).
--
-- Run this yourself after completing the AWS setup in
-- spark_jobs/radio_topic_modeling/README.md, with the placeholders below
-- filled in from that setup. Do not commit real credential values here —
-- keep this file with placeholders in git and paste real values only when
-- running it against your MotherDuck workspace.

-- MotherDuck talks to Glue/S3 with AWS SigV4 (access key/secret, or any
-- other AWS credential type DuckDB supports — credential chain, SSO, IAM
-- role), not the OAuth2/bearer-token auth other REST catalogs use, so this
-- is a generic S3 secret rather than a TYPE ICEBERG one.
CREATE SECRET radio_lakehouse_secret IN MOTHERDUCK (
    TYPE S3,
    KEY_ID '<AWS_ACCESS_KEY_ID>',
    SECRET '<AWS_SECRET_ACCESS_KEY>',
    REGION '<AWS_REGION>'
);

CREATE DATABASE radio_lakehouse (
    TYPE ICEBERG,
    ENDPOINT_TYPE 'glue',
    -- Glue's "warehouse" here is the catalog id, which for a standard (not
    -- Lake Formation cross-account) Glue Data Catalog is just your AWS
    -- account id — NOT an S3 path. The S3 location of each table is
    -- already recorded in Glue's own table metadata, set at table-creation
    -- time by job.py's spark.sql.catalog.<name>.warehouse config.
    WAREHOUSE '<AWS_ACCOUNT_ID>',
    "secret" radio_lakehouse_secret,
    -- The Glue database job.py creates tables under (its ICEBERG_NAMESPACE,
    -- "raw" by default).
    DEFAULT_SCHEMA 'raw',
    READ_ONLY true
);

-- Sanity check once both statements above have run:
-- SELECT * FROM radio_lakehouse.raw.radio_messages LIMIT 10;
-- SELECT * FROM radio_lakehouse.raw.radio_topics;
