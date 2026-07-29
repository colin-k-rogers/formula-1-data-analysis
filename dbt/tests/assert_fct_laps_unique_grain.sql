-- fct_laps should have exactly one row per session/driver/lap
select session_key, driver_number, lap_number, count(*) as n
from {{ ref('fct_laps') }}
group by 1, 2, 3
having count(*) > 1
