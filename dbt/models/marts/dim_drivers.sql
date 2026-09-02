-- One row per driver PER SESSION, not collapsed to one row per driver:
-- drivers change teams between seasons (and occasionally mid-season), so a
-- driver's team_name/team_colour is only accurate as of a given session, not
-- as a single fixed fact. Consumers join on (driver_number, session_key) to
-- get the team a driver was actually racing for at that specific race.
with drivers as (
    select
        *,
        row_number() over (
            partition by driver_number, session_key
            order by full_name
        ) as rn
    from {{ ref('stg_openf1__drivers') }}
)

select
    driver_number,
    session_key,
    full_name,
    broadcast_name,
    name_acronym,
    team_name,
    team_colour
from drivers
where rn = 1
