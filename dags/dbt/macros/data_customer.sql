{% macro select_customer() %}
    CAST(s.customer_id AS TEXT) AS customer_id,
    CAST(s.customer_unique_id AS TEXT) AS customer_unique_id,
    CAST(s.customer_zip_code_prefix AS BIGINT) AS customer_zip_code_prefix,
    CAST(s.customer_city AS TEXT) AS customer_city,
    CAST(s.customer_state AS TEXT) AS customer_state
{% endmacro %}