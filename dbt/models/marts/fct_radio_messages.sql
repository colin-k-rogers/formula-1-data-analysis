-- Each radio message tagged with the lap in progress when it was sent (the
-- last lap that had already started as of message_date), plus driver/session
-- dims and its topic under both classifications -- BERTopic's discovered
-- cluster and prompt_jev's fixed label (see int_radio__jev_topics) -- so a
-- message can be placed on the lap timeline of a race, on the topic timeline
-- of a season, and compared between the two classifiers.
with messages as (
    select * from {{ ref('stg_radio__messages') }}
),

laps as (
    select * from {{ ref('fct_laps') }}
),

sessions as (
    select * from {{ ref('dim_sessions') }}
),

drivers as (
    select * from {{ ref('dim_drivers') }}
),

topics as (
    select * from {{ ref('dim_radio_topics') }}
),

jev as (
    select * from {{ ref('int_radio__jev_topics') }}
),

-- Carries every column of `messages` through (not just the join key) so the
-- final select doesn't need to join back to `messages` a second time just
-- to re-fetch the columns it already has here.
message_laps as (
    select
        m.*,
        l.lap_number,
        row_number() over (
            partition by m.radio_message_id
            order by l.date_start desc
        ) as rn
    from messages m
    left join laps l
        on m.session_key = l.session_key
        and m.driver_number = l.driver_number
        and l.date_start <= m.message_date
    qualify rn = 1
)

select
    m.radio_message_id,
    m.session_key,
    se.year,
    se.country_name,
    se.circuit_short_name,
    se.meeting_official_name,
    se.session_name,
    se.date_start as session_date,
    m.driver_number,
    d.full_name as driver_full_name,
    d.name_acronym as driver_acronym,
    d.team_name,
    d.team_colour,
    m.lap_number,
    m.message_date,
    m.transcript_text,
    m.language,
    m.duration_sec,
    m.topic_id,
    coalesce(t.topic_label, 'Uncategorized') as topic_label,
    t.top_keywords as topic_keywords,
    j.jev_topic,
    -- Display names are resolved here rather than in the incremental model
    -- upstream, so renaming a topic is a rebuild of this table instead of a
    -- paid reclassification of every message.
    {{ jev_display_name(jev_radio_topics(), 'j.jev_topic') }} as jev_topic_label,
    j.jev_topic_confidence,
    j.jev_speech_act,
    {{ jev_display_name(jev_radio_speech_acts(), 'j.jev_speech_act') }} as jev_speech_act_label,
    j.jev_speech_act_confidence
from message_laps m
left join sessions se on m.session_key = se.session_key
left join drivers d
    on m.driver_number = d.driver_number
    and m.session_key = d.session_key
left join topics t on m.topic_id = t.topic_id
left join jev j on m.radio_message_id = j.radio_message_id
