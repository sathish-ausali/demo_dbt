{% macro decimal_test(model, column_name, scale) %}

with validation as (

    select
        cast({{ column_name }} as string) as even_field
    from {{ model }}

),

validation_check as (
	select
        even_field,
        {{ dbt_utils.split_part("even_field", "'.'", 2) }} as decimal_scale_field
from validation	
),

validation_errors as (

    select
	even_field,
	{{ dbt_utils.length("decimal_scale_field") }} as decimal_value
from validation_check
where {{ dbt_utils.length("decimal_scale_field") }} != {{ scale }}
)

select *
from validation_errors

{% endmacro %}
