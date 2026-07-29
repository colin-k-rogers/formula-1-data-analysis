with sessions as (
    select * from {{ ref('stg_openf1__sessions') }}
),

meetings as (
    select * from {{ ref('stg_openf1__meetings') }}
)

select
    s.session_key,
    s.meeting_key,
    m.year,
    m.country_name,
    m.circuit_short_name,
    m.meeting_official_name,
    s.session_name,
    s.date_start,
    s.date_end
from sessions s
left join meetings m on s.meeting_key = m.meeting_key
