with source as (
    select * from {{ source('openf1', 'laps') }}
)

select
    session_key,
    driver_number,
    lap_number,
    cast(date_start as timestamptz) as date_start,
    lap_duration,
    duration_sector_1,
    duration_sector_2,
    duration_sector_3,
    i1_speed,
    i2_speed,
    st_speed,
    is_pit_out_lap
from source
