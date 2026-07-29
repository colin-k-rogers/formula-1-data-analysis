with source as (
    select * from {{ source('openf1', 'drivers') }}
)

select
    session_key,
    driver_number,
    full_name,
    broadcast_name,
    name_acronym,
    team_name,
    team_colour
from source
