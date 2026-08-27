with source as (
    select * from {{ source('radio', 'radio_topics') }}
)

select
    topic_id,
    label,
    top_keywords,
    doc_count
from source
-- BERTopic reserves topic_id = -1 for outlier/unclustered messages; keep it
-- out of dim_radio_topics since it isn't a real topic, but stg_radio__messages
-- still carries topic_id = -1 through so those messages aren't dropped.
where topic_id != -1
