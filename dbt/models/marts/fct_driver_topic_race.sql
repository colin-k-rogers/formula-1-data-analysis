-- Driver x session x topic grain: how many radio messages of each topic a
-- driver had in a given race, and what share of that driver's radio traffic
-- that topic made up. This is the grain the season-evolution Dive charts
-- (topic mix per driver/team across races) query directly.
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
        session_date,
        driver_number,
        driver_full_name,
        driver_acronym,
        team_name,
        team_colour,
        topic_id,
        topic_label,
        count(*) as message_count
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
    c.session_date,
    c.driver_number,
    c.driver_full_name,
    c.driver_acronym,
    c.team_name,
    c.team_colour,
    c.topic_id,
    c.topic_label,
    c.message_count,
    dt.driver_total_messages,
    c.message_count / dt.driver_total_messages as share_of_driver_messages
from counts c
join driver_totals dt
    on c.session_key = dt.session_key
    and c.driver_number = dt.driver_number
