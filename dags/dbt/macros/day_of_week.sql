{% macro day_of_week(column) %}
case
        when {{ column }}  = 1 then 'monday'
        when {{ column }}  = 2 then 'tuesday'
        when {{ column }}  = 3 then 'wednesday'
        when {{ column }}  = 4 then 'thursday'
        when {{ column }}  = 5 then 'friday'
        when {{ column }}  = 6 then 'saturday'
        when {{ column }}  = 7 then 'sunday'
        else 'unknown'
end
{% endmacro %}