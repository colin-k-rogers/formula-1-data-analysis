-- fct_driver_jev_topic_race should have exactly one row per
-- session/driver/topic. Keyed on topic_label, not a topic id: the Jev
-- taxonomy's labels are its ids (see macros/jev_radio_taxonomy.sql).
select session_key, driver_number, topic_label, count(*) as n
from {{ ref('fct_driver_jev_topic_race') }}
group by 1, 2, 3
having count(*) > 1
