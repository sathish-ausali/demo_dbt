How to call the function --> {{ta_window_fun(Table_Name,'Type of Window Function',[partition columns in the form of array],[ORDER BY columns in the form of array],'Sequence like Asc/Desc','Offset value for lead and lag function only','Default Value for lead and lag only')}}


{%- macro ta_window_fun(source,window_fun,partition_col,order_col,sequence,offset = '0',default_arg = 'NULL' ) -%}

{%- set par_len = (partition_col|length ) -%}
{%- set order_len = (order_col|length ) -%}

with source as ( select *,
                {% if window_fun == 'DENSE_RANK' or window_fun == 'dense_rank' %}
                        DENSE_RANK() OVER (
                        {% if par_len > 0 %}
                                PARTITION BY  {% for column in ( partition_col ) %}
                                                                                {{column}}
                                                    {% if not loop.last %},
                                                                                                        {% endif %}
                                              {% endfor %}
                        {% else %}
                        {% endif %}
                        {% if order_len > 0 %}
                                ORDER BY {% for column in ( order_col ) %}
                                                                                                                        {{column}}
                                                                                                                        {% if not loop.last %},{% endif %}
                          {% endfor %}
                        {% else %}
                        {% endif %}
                        {{ sequence }}

                        ) as {{window_fun }}
                {% elif window_fun == 'RANK' or window_fun == 'rank' %}
                        RANK() OVER (
                                                {% if par_len > 0 %}
                                PARTITION BY  {% for column in ( partition_col ) %}
                                              {{column}}
                                              {% if not loop.last %},{% endif %}
                                              {% endfor %}
                        {% else %}
                        {% endif %}
                        {% if order_len > 0 %}
                                ORDER BY {% for column in ( order_col ) %}
                                                                {{column}}
                                {% if not loop.last %},
                                                                {% endif %}
                                                                {% endfor %}
                        {% else %}
                        {% endif %}
                        {{ sequence }}
                        )as {{window_fun }}
                                {% elif window_fun == 'LEAD' or window_fun == 'lead' %}
                        LEAD ({{default_arg}},{{offset}}) OVER (
                        {% if par_len > 0 %}
                                PARTITION BY  {% for column in ( partition_col ) %}
                                              {{column}}
                                              {% if not loop.last %},{% endif %}
                                              {% endfor %}
                        {% else %}
                        {% endif %}
                        {% if order_len > 0 %}
                                ORDER BY {% for column in ( order_col ) %}
                                                                {{column}}
                                {% if not loop.last %},{% endif %}
                                                                {% endfor %}
                        {% else %}
                        {% endif %}
                        )as {{window_fun }}
                                {% elif window_fun == 'LAG'or window_fun == 'lag' %}
                        LAG ({{default_arg}},{{offset}}) OVER (
                        {% if par_len > 0 %}
                                PARTITION BY  {% for column in ( partition_col ) %}
                                              {{column}}
                                              {% if not loop.last %},{% endif %}
                                              {% endfor %}
                        {% else %}
                        {% endif %}
                        {% if order_len > 0 %}
                                ORDER BY {% for column in ( order_col ) %}
                                                                {{column}}
                                {% if not loop.last %},{% endif %}
                                                                {% endfor %}
                        {% else %}
                        {% endif %}
                        )as {{ window_fun }}
                {% else %}
                        ROW_NUMBER() OVER (
                                                {% if par_len > 0 %}
                                PARTITION BY  {% for column in ( partition_col ) %}
                                              {{column}}
                                              {% if not loop.last %},{% endif %}
                                              {% endfor %}
                        {% else %}
                        {% endif %}
                        {% if order_len > 0 %}
                                ORDER BY {% for column in ( order_col ) %}
                                                                {{column}}
                                {% if not loop.last %},{% endif %}
                                                                {% endfor %}
                        {% else %}
                        {% endif %}
                        {{ sequence }}
                        ) as {{window_fun}}
                                {% endif %}
         from {{ source }}
                   )
select * from source

{%- endmacro -%}