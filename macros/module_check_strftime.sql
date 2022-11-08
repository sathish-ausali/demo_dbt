{% macro module_check_strftime(data, format) %}
{{ return(modules.datetime.datetime.now().strptime("'"+data+"'",format).strftime("'"+format+"'")) }}
{% endmacro %}
