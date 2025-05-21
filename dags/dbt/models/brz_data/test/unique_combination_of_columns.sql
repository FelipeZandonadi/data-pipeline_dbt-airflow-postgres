{% set columns = kwargs.get('combination_of') %}
SELECT
    {{ columns | join(', ') }},
    COUNT(*) as count
FROM {{ model }}
GROUP BY {{ columns | join(', ') }}
HAVING COUNT(*) > 1
