{{ config(
    materialized='table',
    alias='slv_tb_customers',
    post_hook='ALTER TABLE slv_data.slv_tb_customers ADD PRIMARY KEY (CD_customer)'
) }}

with source as (
    SELECT *
    FROM {{ source('brz_data', 'brz_tb_customers') }} AS btc
)

SELECT 
    CAST(s.customer_id AS TEXT) AS  CD_customer,
    CAST(s.customer_unique_id AS TEXT) AS CD_customer_unique,
    CAST(s.customer_zip_code_prefix AS BIGINT) AS CD_zip_code_prefix,
    CAST(s.customer_city AS TEXT) AS NM_city,
    CAST(s.customer_state AS TEXT) AS NM_state
FROM source AS s