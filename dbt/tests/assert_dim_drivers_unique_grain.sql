-- dim_drivers should have exactly one row per driver/session (the grain is
-- deliberately per-session, not per-driver, since a driver's team can change
-- between sessions/seasons)
select driver_number, session_key, count(*) as n
from {{ ref('dim_drivers') }}
group by 1, 2
having count(*) > 1
