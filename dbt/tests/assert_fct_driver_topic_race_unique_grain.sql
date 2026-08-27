-- fct_driver_topic_race should have exactly one row per session/driver/topic
select session_key, driver_number, topic_id, count(*) as n
from {{ ref('fct_driver_topic_race') }}
group by 1, 2, 3
having count(*) > 1
