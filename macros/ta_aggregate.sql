{%- macro ta_aggregate(source,Agg_fun,Source_column,Source_group_col ) -%}
{%- set src_len = (Source_group_col|length ) -%}

with source as ( select
        {% for S_column in ( Source_group_col ) %}
          {{S_column}} as S_{{S_column}},
        {% endfor %}
    {% if Agg_fun == 'count' or Agg_fun == 'COUNT' %}
    count({{ Source_column }}) as {{Agg_fun}}_{{Source_column}}
    {% elif Agg_fun == 'sum' or Agg_fun == 'SUM' %}
        SUM({{ Source_column }}) as {{Agg_fun}}_{{Source_column}}
    {% elif Agg_fun == 'average' or Agg_fun == 'AVERAGE' %}
        AVG({{ Source_column }}) as {{Agg_fun}}_{{Source_column}}
    {% elif Agg_fun == 'min' or Agg_fun == 'MIN' %}
        min({{ Source_column }}) as {{Agg_fun}}_{{Source_column}}
    {% elif Agg_fun == 'max' or Agg_fun == 'MAX' %}
        max({{ Source_column }}) as {{Agg_fun}}_{{Source_column}}
    {% else %}
    COUNT( DISTINCT {{ Source_column }} )  as {{Agg_fun}}_{{Source_column}}
    {% endif %}
from {{ source }} {% if src_len > 0 %} {{ dbt_utils.group_by(src_len) }}
{% else %}
{% endif %}

)

select * from source
{%- endmacro -%}