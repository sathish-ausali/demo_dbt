{% macro ta_is_masked(model, column_name, mask, threshold) %}


with cast_validation as (
	select 
	cast({{ column_name }} as string) as column_cast	
	from {{ model }}
),

validation as (
	select
       column_cast,	
	{{ dbt_utils.length("column_cast") }} as column_length,
	{{ dbt_utils.length(dbt_utils.replace("column_cast", "'"+mask+"'", "''")) }} as unmasked_character_length
	from cast_validation
),

validation_error as (
	select *      
	from validation
	where 
	cast(((column_length - unmasked_character_length) * 100 / column_length) as int) < {{ threshold }}
)

select *
from validation_error

{% endmacro %}
