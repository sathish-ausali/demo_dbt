{%- macro ta_agg(column,agg_fun) -%}

    {% if agg_fun == 'count' %}
    count({{ column }}) as count_{{column}}
    {% elif agg_fun == 'sum' %}
        SUM({{ column }}) as sum_{{column}}
    {% elif agg_fun == 'average' %}
        AVG({{ column }}) as avg_{{column}}
    {% else %}
    COUNT( DISTINCT {{ column }} )  as DISTINCT_{{column}}
    {% endif %}

{%- endmacro -%}
