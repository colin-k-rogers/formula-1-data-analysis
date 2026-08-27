with source as (
    select * from {{ source('radio', 'radio_messages') }}
)

select
    radio_message_id,
    session_key,
    meeting_key,
    driver_number,
    cast(message_date as timestamptz) as message_date,
    transcript_text,
    language,
    duration_sec,
    topic_id
from source
where transcribe_error is null
  and transcript_text is not null
