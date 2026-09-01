select
    topic_id,
    label as topic_label,
    top_keywords,
    doc_count
from {{ ref('stg_radio__topics') }}
