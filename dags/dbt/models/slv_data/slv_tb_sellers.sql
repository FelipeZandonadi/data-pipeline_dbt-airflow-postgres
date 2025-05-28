{{ config(
    materialized='table',
    alias='slv_tb_sellers',
    post_hook=[
        "ALTER TABLE slv_data.slv_tb_sellers ADD PRIMARY KEY (seller_id)",
        ]
) }}

with source as (
    SELECT
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state
    FROM {{ source('brz_data', 'brz_tb_sellers') }}
)

SELECT 
    CAST(s.seller_id AS TEXT) AS seller_id,
    CAST(s.seller_zip_code_prefix AS INTEGER) AS seller_zip_code_prefix,
    CAST(s.seller_city AS TEXT) AS seller_city,
    CAST(s.seller_state AS TEXT) AS seller_state
FROM source AS s