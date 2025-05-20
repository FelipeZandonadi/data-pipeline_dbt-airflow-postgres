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
    {{ select_customer() }}
FROM source AS s