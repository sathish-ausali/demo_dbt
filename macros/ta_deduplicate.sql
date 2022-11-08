{% macro ta_deduplicate(model, column_name, column_list) %}

{%- set column_name_list = column_list | list -%}
{%- set column_length = column_list | length -%}

SELECT 
{% if column_list is defined and column_list|length > 0 %}
{{ column_name }}, {{ column_name_list }}
{% else %}
{{ column_name }}
{% endif %}
    FROM {{ model }}
{% if column_list is defined and column_list|length > 0 %}
{{ dbt_utils.group_by(column_length + 1) }}
{% else %}
{{ dbt_utils.group_by(1) }}	
{% endif %}
  HAVING COUNT(*) > 1
{% endmacro %}
