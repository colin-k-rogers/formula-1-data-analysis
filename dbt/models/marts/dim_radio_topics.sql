with topics as (
    select * from {{ ref('stg_radio__topics') }}
),

overrides as (
    select * from {{ ref('topic_name_overrides') }}
),

-- topic_id isn't a stable join key -- BERTopic renumbers/reshuffles topics
-- on every FORCE_REFIT, so overrides are matched by keyword content
-- instead (see seeds/topic_name_overrides.csv). A topic's top_keywords
-- could in principle contain more than one override's keyword; keep only
-- the longest (most specific) match so this stays one row per topic_id.
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
    -- Falls back to BERTopic's raw keyword-soup label when a topic has no
    -- curated override yet (e.g. a fresh FORCE_REFIT reshuffled topics
    -- before the seed was updated for them) instead of showing a blank name.
    coalesce(display_name, label) as topic_label,
    top_keywords,
    doc_count
from matched
where rn = 1
