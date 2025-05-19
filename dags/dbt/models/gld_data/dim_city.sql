{{ config(
    materialized='table',
    alias='dim_city',
    post_hook=[
        'ALTER TABLE gld_data.dim_city ADD PRIMARY KEY (city_key)'
    ]
)}}

WITH customer_location AS (
    SELECT DISTINCT
        coalesce(stc.customer_city, 'unknown') city,
        coalesce(stc.customer_state, 'unknown') state
    FROM {{ source('slv_data', 'slv_tb_customers') }} stc

)

SELECT
    md5(cl.city || cl.state) city_key,
    cl.city,
    cl.state
FROM customer_location cl
