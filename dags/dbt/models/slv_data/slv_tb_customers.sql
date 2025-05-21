{{ config(
    materialized='table',
    alias='slv_tb_customers',
    post_hook='ALTER TABLE slv_data.slv_tb_customers ADD PRIMARY KEY (customer_id)'
) }}

with source as (
    SELECT *
    FROM {{ source('brz_data', 'brz_tb_customers') }} AS btc
)

SELECT 
    CAST(s.customer_id AS TEXT) AS customer_id,
    CAST(s.customer_unique_id AS TEXT) AS customer_unique_id,
    CAST(s.customer_zip_code_prefix AS BIGINT) AS customer_zip_code_prefix,
    CAST(s.customer_city AS TEXT) AS customer_city,
    CAST(s.customer_state AS TEXT) AS customer_state
FROM source AS s