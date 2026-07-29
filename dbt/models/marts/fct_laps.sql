-- Excludes pit-out laps and null/zero durations, which aren't representative
-- of green-flag race pace and would distort relative-pace comparisons.
select
    session_key,
    driver_number,
    lap_number,
    date_start,
    lap_duration,
    duration_sector_1,
    duration_sector_2,
    duration_sector_3,
    i1_speed,
    i2_speed,
    st_speed
from {{ ref('stg_openf1__laps') }}
where coalesce(is_pit_out_lap, false) = false
  and lap_duration is not null
  and lap_duration > 0
