-- Every label prompt_jev returned must still exist in
-- macros/jev_radio_taxonomy.sql. int_radio__jev_topics is incremental, so
-- removing or renaming a label there leaves the old one on rows classified
-- before the change -- rows the Dive would then chart under a name nothing
-- else in the project defines. Failing here is the signal to rebuild the
-- model with `--full-refresh`.
select jev_topic as label, 'topic' as taxonomy, count(*) as n
from {{ ref('int_radio__jev_topics') }}
where jev_topic not in ({{ jev_label_list(jev_radio_topics()) }})
group by 1, 2

union all

select jev_speech_act as label, 'speech_act' as taxonomy, count(*) as n
from {{ ref('int_radio__jev_topics') }}
where jev_speech_act not in ({{ jev_label_list(jev_radio_speech_acts()) }})
group by 1, 2
