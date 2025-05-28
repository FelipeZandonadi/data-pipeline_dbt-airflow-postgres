{% macro catch_date_information(column) %}
    CAST({{ column }} AS DATE) AS date,
    extract(YEAR FROM {{ column }}) AS year,
    extract(MONTH FROM {{ column }}) AS month,
    extract(DAY FROM {{ column }}) AS day,
    extract(ISODOW FROM {{ column }}) AS day_week,
    extract(QUARTER FROM {{ column }}) AS quarter
{% endmacro %}