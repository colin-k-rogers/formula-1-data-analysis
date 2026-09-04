-- Each radio message tagged with the lap in progress when it was sent (the
-- last lap that had already started as of message_date), plus driver/session
-- dims and its BERTopic topic, so a message can be placed on both the lap
-- timeline of a race and the topic timeline of a season.
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
    t.top_keywords as topic_keywords
from message_laps m
left join sessions se on m.session_key = se.session_key
left join drivers d
    on m.driver_number = d.driver_number
    and m.session_key = d.session_key
left join topics t on m.topic_id = t.topic_id
