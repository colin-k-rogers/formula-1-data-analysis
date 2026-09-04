with topics as (
    select * from {{ ref('stg_radio__topics') }}
),

overrides as (
    select * from {{ ref('topic_name_overrides') }}
),

-- topic_id isn't stable across FORCE_REFIT, so overrides match on keyword
-- content instead (see seeds/topic_name_overrides.csv). Keep only the
-- longest match if a topic matches more than one override.
matched as (
    select
        t.topic_id,
        t.label,
        t.top_keywords,
        t.doc_count,
        o.display_name,
        row_number() over (
            partition by t.topic_id
            order by length(o.topic_keyword) desc
        ) as rn
    from topics t
    left join overrides o
        on t.top_keywords ilike '%' || o.topic_keyword || '%'
)

select
    topic_id,
    -- Falls back to BERTopic's raw label when no curated override matches yet.
    coalesce(display_name, label) as topic_label,
    top_keywords,
    doc_count
from matched
where rn = 1
