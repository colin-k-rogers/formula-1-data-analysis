{{
    config(
        materialized='incremental',
        unique_key='radio_message_id',
        on_schema_change='append_new_columns'
    )
}}

-- A second, independent topic classification of every radio message, from
-- MotherDuck's prompt_jev (TypeSafe's Jev model) rather than BERTopic. It
-- doesn't replace stg_radio__topics; fct_radio_messages carries both, so the
-- same message can be read under either classification.
--
-- Why it earns its place next to BERTopic: BERTopic clusters, so a message
-- that doesn't fit a cluster lands in topic_id = -1, and roughly 30% of the
-- corpus does. Those rows aren't unclassifiable, just unclustered -- they're
-- full of gaps to rivals, damage reports and weather calls. prompt_jev picks
-- from a fixed list instead of discovering one, so it labels every message
-- and, being a fixed list, never renumbers itself on a refit the way
-- BERTopic's topic ids do (see spark_jobs/radio_topic_modeling FORCE_REFIT).
-- What it gives up is discovery: it can only ever find topics already named
-- in macros/jev_radio_taxonomy.sql, which is exactly what BERTopic is for.
--
-- Incremental because each row here is a paid model call. Only messages whose
-- transcript this table hasn't already seen are sent, so a re-transcribed
-- message reclassifies itself but an unchanged one is never paid for twice.
-- A `--full-refresh` reclassifies the corpus regardless, which is what to run
-- after editing a label or description in the macro -- those change the
-- question rather than the input, so nothing here can detect them.
-- Storing only machine labels, never display names, for the same reason:
-- renaming a topic for the Dive is a marts-layer concern (see
-- fct_radio_messages) and must not require paying to reclassify.

with messages as (
    select
        radio_message_id,
        transcript_text,
        -- A freshness key, not just a shorter transcript. radio_message_id
        -- identifies the *recording* -- the Spark job builds it from the
        -- audio URL and MERGEs rows in place by it -- so an id already in
        -- this table can still be carrying a transcript that has since
        -- changed underneath it, which is exactly what REPROCESS_ALL after a
        -- WHISPER_MODEL_SIZE change does to a whole season. Matching on the
        -- id alone would skip every one of those rows and leave their labels
        -- pinned to a transcript they no longer have, while BERTopic's
        -- labels for the same rows got refreshed.
        md5(transcript_text) as transcript_hash
    from {{ ref('stg_radio__messages') }}
),

new_messages as (
    select m.*
    from messages m
    {% if is_incremental() %}
    left join {{ this }} existing
        on m.radio_message_id = existing.radio_message_id
        and m.transcript_hash = existing.transcript_hash
    where existing.radio_message_id is null
    {% endif %}
),

-- MATERIALIZED is load-bearing, not a hint: without it DuckDB may inline the
-- CTE and re-evaluate prompt_jev once per struct field read below, calling
-- the model four times per message instead of twice.
classified as materialized (
    select
        radio_message_id,
        transcript_hash,
        prompt_jev(
            transcript_text,
            'This is a Formula 1 team radio message between a driver and their race engineer. Which single subject does it mainly cover?',
            choice := {{ jev_choice_array(jev_radio_topics()) }}
        ) as topic,
        prompt_jev(
            transcript_text,
            'A Formula 1 race engineer and their driver are talking on team radio. What is the speaker mainly doing in this message?',
            choice := {{ jev_choice_array(jev_radio_speech_acts()) }}
        ) as speech_act
    from new_messages
)

select
    radio_message_id,
    -- Carried so the next incremental run can tell "already classified" from
    -- "already classified, but from a transcript that has since changed".
    transcript_hash,
    topic.choice as jev_topic,
    -- How sure the model is of the winning label, 0-1. Kept alongside the
    -- label because the label alone can't be filtered on quality: this is
    -- the closest analogue to BERTopic's outlier bucket, but as a dial
    -- rather than a bucket. Roughly 70% of messages land at 0.6 or above.
    topic.confidence as jev_topic_confidence,
    speech_act.choice as jev_speech_act,
    speech_act.confidence as jev_speech_act_confidence
from classified
