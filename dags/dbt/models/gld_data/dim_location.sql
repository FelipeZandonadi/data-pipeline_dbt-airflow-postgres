{{ config(
    materialized='table',
    alias='dim_location',
    post_hook=[
        'ALTER TABLE gld_data.dim_location ADD PRIMARY KEY (id_state_city_sk)'
    ]
)}}

WITH customer_location AS (
    SELECT DISTINCT
        coalesce(customer_state, 'unknown') state,
        coalesce(customer_city, 'unknown') city
    FROM {{ source('slv_data', 'slv_tb_customers') }}

)

SELECT
    cl.state || '_' || cl.city id_state_city_sk,
    cl.state,
    cl.city
FROM customer_location cl