-- fct_driver_topic_race's counterpart under the prompt_jev classification:
-- driver x session x topic message counts and share of that driver's radio
-- traffic, built from fct_radio_messages.jev_topic instead of the BERTopic
-- topic.
--
-- The topic column is deliberately named `topic_label`, not `jev_topic_label`
-- as it is upstream: this table is column-for-column interchangeable with
-- fct_driver_topic_race (minus topic_id, which has no Jev equivalent -- the
-- labels are the ids here), so the Dive's classifier toggle swaps one table
-- name and every query and chart downstream keeps working unchanged.

with messages as (
    select * from {{ ref('fct_radio_messages') }}
),

counts as (
    select
        session_key,
        year,
        country_name,
        circuit_short_name,
        meeting_official_name,
        session_name,
        session_date,
        driver_number,
        driver_full_name,
        driver_acronym,
        team_name,
        team_colour,
        jev_topic_label as topic_label,
        count(*) as message_count,
        -- Averaged over the messages behind each bar, so a slice built out
        -- of calls the classifier was unsure about can be told apart from
        -- one it was certain of -- the BERTopic mart has no equivalent
        -- (a message is in a cluster or in the outlier bucket, full stop).
        avg(jev_topic_confidence) as avg_confidence
    from messages
    group by all
),

driver_totals as (
    select
        session_key,
        driver_number,
        sum(message_count) as driver_total_messages
    from counts
    group by all
)

select
    c.session_key,
    c.year,
    c.country_name,
    c.circuit_short_name,
    c.meeting_official_name,
    c.session_name,
    c.session_date,
    c.driver_number,
    c.driver_full_name,
    c.driver_acronym,
    c.team_name,
    c.team_colour,
    c.topic_label,
    c.message_count,
    c.avg_confidence,
    dt.driver_total_messages,
    c.message_count / dt.driver_total_messages as share_of_driver_messages
from counts c
join driver_totals dt
    on c.session_key = dt.session_key
    and c.driver_number = dt.driver_number
