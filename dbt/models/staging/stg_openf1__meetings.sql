with source as (
    select * from {{ source('openf1', 'meetings') }}
)

select
    meeting_key,
    year,
    country_name,
    circuit_short_name,
    meeting_official_name,
    meeting_name,
    location,
    cast(date_start as timestamptz) as date_start,
    cast(date_end as timestamptz) as date_end
from source
