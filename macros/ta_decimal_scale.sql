{% test ta_decimal_scale(model, column_name, scale) %}

with validation as (

    select
        cast({{ column_name }} as string) as decimal_field
    from {{ model }}

),

validation_check as (
        select
        decimal_field,
        {{ dbt_utils.split_part("decimal_field", "'.'", 2) }} as decimal_scale_field
from validation
),

validation_errors as (

    select
        decimal_field
from validation_check
where {{ dbt_utils.length("decimal_scale_field") }} != {{ scale }}
)

select *
from validation_errors

{% endtest %}
