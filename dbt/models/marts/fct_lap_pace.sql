-- Core mart for relative pace analysis: each driver's lap time compared to
-- the fastest and median lap turned by the whole field on that same lap of
-- that same session, so drivers are compared under identical track/weather
-- conditions rather than across the whole race distance.
--
-- Race sessions only: Qualifying is flying laps with no stable "gap to the
-- field", and Sprint is a shorter, differently-fueled race distance -- neither
-- is comparable lap-for-lap to Race pace. Filtered here, in the mart that
-- actually needs the invariant, rather than relied upon staying true of
-- fct_laps/f1.raw.laps upstream (which now also carries Qualifying/Sprint
-- laps for fct_radio_messages's lap-number lookups).
with laps as (
    select l.*
    from {{ ref('fct_laps') }} l
    join {{ ref('dim_sessions') }} se on l.session_key = se.session_key
    where se.session_name = 'Race'
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
left join {{ ref('dim_drivers') }} d
    on l.driver_number = d.driver_number
    and l.session_key = d.session_key
