with source as (
    select * from {{ source('openf1', 'sessions') }}
)

select
    session_key,
    meeting_key,
    session_name,
    session_type,
    year,
    cast(date_start as timestamptz) as date_start,
    cast(date_end as timestamptz) as date_end
from source
