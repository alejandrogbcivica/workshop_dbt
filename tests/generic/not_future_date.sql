{% test not_future_date(model, column_name, allow_today=true) %}

with data as (
    select cast({{ column_name }} as date) as d
    from {{ model }}
)

select d
from data
where d is not null
  and (
        d > current_date()
        {% if not allow_today %} or d = current_date() {% endif %}
      )

{% endtest %}