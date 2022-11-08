{% macro module_strftime(col, format) %}

	{{ return(modules.datetime.datetime.now().strptime(col, format).strftime("'"+format+"'")) }}
{% endmacro %}

{% macro date_format_check(model, column_name, format) %}

{% set payment_methods = dbt_utils.get_column_values(
    table=ref('date_data'),
    column='date_string'
) %}

select
{{ column_name }},
{% for col in payment_methods %}
{% if "'"+col+"'" != module_strftime(col, format) %}
{{ exceptions.raise_compiler_error("Invalid `format`. Got: " ~ col) }}
{% endif %}
{% endfor %}
from {{ model }}

{% endmacro %}
