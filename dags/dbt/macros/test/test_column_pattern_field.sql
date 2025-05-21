{% test column_pattern_field(model, column, pattern) %}

SELECT *
FROM {{ model }}
WHERE {{ column }} NOT LIKE '{{ pattern  }}'

{% endtest %}