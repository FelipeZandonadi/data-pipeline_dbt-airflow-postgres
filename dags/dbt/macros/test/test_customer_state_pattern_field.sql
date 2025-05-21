{% test customer_state_pattern_field(model, column) %}

SELECT *
FROM {{ model }}
WHERE {{ column }} NOT LIKE '__'

{% endtest %}