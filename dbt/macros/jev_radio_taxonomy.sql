{#
  The taxonomies int_radio__jev_topics classifies team radio against with
  MotherDuck's prompt_jev, plus the helpers that render them into SQL.

  Everything about a label lives on one entry here -- the machine label the
  model picks, the description that tells prompt_jev what it means, and the
  human display name the Dive shows -- because prompt_jev needs the first two
  as a query-time constant while the third is only ever a presentation
  detail, and splitting them across a macro and a seed would let the two
  drift into disagreement about which labels exist.

  Changing any `description` re-teaches the classifier, so existing rows stay
  labelled under the old wording until int_radio__jev_topics is rebuilt with
  `--full-refresh`.
#}

{#
  What the message is *about*. Deliberately overlaps BERTopic's own topics
  (see seeds/topic_name_overrides.csv) rather than inventing a parallel
  vocabulary, so the two classifications of the same message can be compared
  directly. Unlike BERTopic these are fixed: they don't reshuffle when the
  model is refit, so no topic_id-to-name reconciliation step is needed.

  Descriptions carry the boundaries that measurably moved confidence during
  development -- pit_stop vs race_strategy, and frustration_or_complaint
  deferring to whatever concrete subject a complaint is about -- not restated
  definitions of the label text.
#}
{% macro jev_radio_topics() %}
    {{ return([
        {'label': 'pit_stop', 'display': 'Pit Stop',
         'description': 'A stop that is happening or imminent: box calls, in-lap and out-lap, pit lane, stop execution, pit-lane traffic'},
        {'label': 'race_strategy', 'display': 'Race Strategy',
         'description': "Deciding when or whether to stop and what the plan is: Plan A/B/C, stint length, pit windows, undercut or overcut, one-stop versus two-stop, what rivals' strategies mean for us"},
        {'label': 'tyres', 'display': 'Tyres',
         'description': 'Tyre compound, wear, degradation, temperature, warm-up, flat spots, grip from the tyres'},
        {'label': 'pace_and_gaps', 'display': 'Pace & Gaps',
         'description': 'Lap times, deltas, target times, gap to the car ahead or behind, current position'},
        {'label': 'push_or_manage', 'display': 'Push or Manage',
         'description': 'Instruction on how hard to go now: push, attack, lift and coast, manage to the end, save the car'},
        {'label': 'driving_technique', 'display': 'Driving Technique',
         'description': 'Corner-by-corner or sector-by-sector coaching: braking points, lines, where time is gained or lost'},
        {'label': 'car_handling', 'display': 'Car Handling',
         'description': 'Balance, understeer or oversteer, ride, setup changes, switch and engine-mode changes, differential'},
        {'label': 'car_problem', 'display': 'Car Problem',
         'description': 'Damage, mechanical failure, loss of power, warning lights, retirement risk'},
        {'label': 'energy_management', 'display': 'Energy Management',
         'description': 'ERS and battery deployment, recharging, overtake mode, fuel saving'},
        {'label': 'drs_and_overtaking', 'display': 'DRS & Overtaking',
         'description': 'DRS status, overtaking attempts, defending a position, being attacked by another car'},
        {'label': 'weather', 'display': 'Weather',
         'description': 'Rain, wet or drying track, wind, air and track temperature'},
        {'label': 'race_control', 'display': 'Race Control',
         'description': 'Flags, safety car, virtual safety car, red flag, track limits, penalties, stewards'},
        {'label': 'incident', 'display': 'Incident',
         'description': 'A crash, contact, spin, lock-up, off-track moment or near miss involving any car'},
        {'label': 'praise_and_congratulations', 'display': 'Praise & Congratulations',
         'description': 'Well done, great lap, congratulations on a result, thanking the team'},
        {'label': 'frustration_or_complaint', 'display': 'Frustration',
         'description': 'Venting or criticism with no specific technical subject attached; if the driver is complaining about a specific thing such as tyres, the car or a rival, that subject wins instead'},
        {'label': 'acknowledgment_or_chatter', 'display': 'Acknowledgment & Chatter',
         'description': 'Copy, understood, radio checks, wrong channel, banter and small talk carrying no information'},
        {'label': 'other', 'display': 'Other',
         'description': 'None of the above fit'}
    ]) }}
{% endmacro %}


{#
  What the speaker is *doing*, which is orthogonal to the subject: it's what
  separates an engineer instructing on tyres from a driver reporting a tyre
  problem. BERTopic can't express this at all -- it only ever assigns one
  cluster per message, so speech act and subject compete for the same slot
  (its "Radio Acknowledgment" and "Frustration" topics are speech acts
  sitting in a list of subjects).
#}
{% macro jev_radio_speech_acts() %}
    {{ return([
        {'label': 'instructing', 'display': 'Instructing',
         'description': 'Telling the driver to do something now: a command, a call, a setting change'},
        {'label': 'informing', 'display': 'Informing',
         'description': 'Passing on facts with no action attached: times, positions, what rivals are doing, conditions'},
        {'label': 'asking', 'display': 'Asking',
         'description': 'Requesting information or a decision from the other person'},
        {'label': 'reporting', 'display': 'Reporting',
         'description': 'The driver describing how the car feels or what just happened to them'},
        {'label': 'encouraging', 'display': 'Encouraging',
         'description': 'Motivating, reassuring, praising or thanking'},
        {'label': 'venting', 'display': 'Venting',
         'description': 'Complaining, swearing or criticising, with no request or fact attached'},
        {'label': 'acknowledging', 'display': 'Acknowledging',
         'description': 'Only confirming receipt: copy, understood, okay, thanks'}
    ]) }}
{% endmacro %}


{#
  Renders a taxonomy as prompt_jev's `choice :=` argument. Single-quoted SQL
  literals, so any apostrophe in a description has to be doubled.
#}
{% macro jev_choice_array(entries) -%}
[
        {%- for e in entries %}
        {label: '{{ e.label }}', description: '{{ e.description | replace("'", "''") }}'}{{ "," if not loop.last else "" }}
        {%- endfor %}
    ]
{%- endmacro %}


{#
  Renders the machine-label-to-display-name mapping as a CASE expression.
  `else` is the raw label rather than NULL so a label that somehow escapes
  this list still shows up in the Dive (ugly, but visible) instead of
  silently becoming an unnamed slice of every chart.
#}
{% macro jev_display_name(entries, column_name) -%}
case {{ column_name }}
        {%- for e in entries %}
        when '{{ e.label }}' then '{{ e.display | replace("'", "''") }}'
        {%- endfor %}
        else {{ column_name }}
    end
{%- endmacro %}


{#
  Renders a taxonomy's machine labels as a quoted SQL list, for `in (...)`
  checks. Used by tests/assert_jev_labels_in_taxonomy.sql -- schema.yml's
  accepted_values can't call project macros, so the guard that prompt_jev
  only ever returns labels this project knows about has to be a singular
  test instead.
#}
{% macro jev_label_list(entries) -%}
{% for e in entries %}'{{ e.label }}'{{ ", " if not loop.last else "" }}{% endfor %}
{%- endmacro %}
