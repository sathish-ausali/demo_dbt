{% macro ta_date_format(model, column_name, format) %}

with validation as (
	select 
		cast({{ column_name }} as string) as date_string,
		cast({{ column_name }} as datetime) as date_column,
		cast({{ column_name }} as {{ dbt_utils.type_timestamp() }}) as date_string_check,
		cast({{ column_name }} as {{ dbt_utils.type_string() }}) as date_column_check
	from {{ model }}
),

validation_error as (
	select *
	from validation
	where date_string = {{ modules.datetime.datetime.now().strptime("'"+date_string+"'", format).strftime("'"+format+"'") }}
)

select * from validation_error

{% endmacro %}
