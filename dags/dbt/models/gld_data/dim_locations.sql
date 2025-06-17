{{ config(
    materialized='table',
    alias='dim_locations',
    post_hook=[
        'ALTER TABLE gld_data.dim_locations ADD PRIMARY KEY (sk_state_city)'
    ]
)}}

WITH customer_location AS (
    SELECT
        COALESCE(nm_state, 'unknown') state,
        COALESCE(nm_city, 'unknown') city
    FROM {{ source("slv_data", "slv_tb_customers") }}
    UNION
    SELECT
        COALESCE(nm_state, 'unknown'),
        COALESCE(nm_city, 'unknown')
    FROM {{ source("slv_data", "slv_tb_sellers") }}

)

SELECT
    cl.state || '-' || cl.city AS SK_state_city,
    cl.state AS NM_state,
    cl.city AS NM_city
FROM customer_location cl