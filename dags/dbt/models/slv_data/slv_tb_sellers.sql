{{ config(
    materialized='table',
    alias='slv_tb_sellers',
    post_hook=[
        "ALTER TABLE slv_data.slv_tb_sellers ADD PRIMARY KEY (CD_seller)",
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
    CAST(s.seller_id AS TEXT) AS CD_seller,
    CAST(s.seller_zip_code_prefix AS INTEGER) AS CD_zip_code_prefix,
    CAST(s.seller_city AS TEXT) AS NM_city,
    CAST(s.seller_state AS TEXT) AS NM_state
FROM source AS s