-- fct_lap_pace should have exactly one row per session/driver/lap
select session_key, driver_number, lap_number, count(*) as n
from {{ ref('fct_lap_pace') }}
group by 1, 2, 3
having count(*) > 1
