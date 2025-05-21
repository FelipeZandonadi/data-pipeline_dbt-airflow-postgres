{% test unique_combination_columns(model, combination_of) %}

SELECT
    {{ combination_of | join(', ') }},
    COUNT(*) as count
FROM {{ model }}
GROUP BY {{ combination_of | join(', ') }}
HAVING COUNT(*) > 1

{% endtest %}