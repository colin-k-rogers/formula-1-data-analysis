-- Team/name info rarely changes intra-season; collapse to one row per
-- driver by taking their most recent session's record.
with drivers as (
    select
        d.*,
        s.date_start as session_date_start,
        row_number() over (
            partition by d.driver_number
            order by s.date_start desc
        ) as rn
    from {{ ref('stg_openf1__drivers') }} d
    left join {{ ref('stg_openf1__sessions') }} s on d.session_key = s.session_key
)

select
    driver_number,
    full_name,
    broadcast_name,
    name_acronym,
    team_name,
    team_colour
from drivers
where rn = 1
