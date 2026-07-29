{#
  dbt's default behavior concatenates the target schema with any custom
  +schema config (e.g. "main_marts"). We want the custom schema name used
  as-is (e.g. "marts"), so models land in f1.marts / f1.staging.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
