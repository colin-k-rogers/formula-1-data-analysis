-- Core mart for relative pace analysis: each driver's lap time compared to
-- the fastest and median lap turned by the whole field on that same lap of
-- that same session, so drivers are compared under identical track/weather
-- conditions rather than across the whole race distance.
with laps as (
    select * from {{ ref('fct_laps') }}
),

lap_stats as (
    select
        session_key,
        lap_number,
        min(lap_duration) as fastest_lap_duration,
        median(lap_duration) as median_lap_duration
    from laps
    group by 1, 2
)

select
    l.session_key,
    se.meeting_key,
    se.year,
    se.country_name,
    se.circuit_short_name,
    se.meeting_official_name,
    se.date_start as session_date,
    l.driver_number,
    d.full_name as driver_full_name,
    d.name_acronym as driver_acronym,
    d.team_name,
    d.team_colour,
    l.lap_number,
    l.lap_duration,
    ls.fastest_lap_duration,
    ls.median_lap_duration,
    l.lap_duration - ls.fastest_lap_duration as delta_to_fastest,
    l.lap_duration - ls.median_lap_duration as delta_to_median,
    rank() over (
        partition by l.session_key, l.lap_number
        order by l.lap_duration asc
    ) as lap_rank
from laps l
join lap_stats ls
    on l.session_key = ls.session_key
    and l.lap_number = ls.lap_number
left join {{ ref('dim_sessions') }} se on l.session_key = se.session_key
left join {{ ref('dim_drivers') }} d on l.driver_number = d.driver_number
