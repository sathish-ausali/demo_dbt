{% test ta_lookup(model, column_name, to, source_table_list, lookup_table_field, lookup_table_list) %}

{%- set source_table_list = source_table_list | list -%}
{%- set source_column_length = source_table_list | length -%}
{%- set lookup_table_list = lookup_table_list | list -%}
{%- set lookup_column_length = lookup_table_list | length -%}

select
    m.{{ column_name }}
from {{ model }} m
left join {{ to }} p
    on m.{{ column_name }} = p.{{ lookup_table_field }}
        {% for i in range(0, source_column_length) %}
                        AND p.{{ lookup_table_list[i] }} = m.{{ source_table_list[i] }}
                {% endfor %}
where p.{{ lookup_table_field }} is null

{% endtest %}
