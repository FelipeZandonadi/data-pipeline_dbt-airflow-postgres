{{ config(
    materialized='table',
    alias='dim_seller',
    post_hook=[
        'ALTER TABLE gld_data.dim_seller ADD PRIMARY KEY (seller_key)'
    ]
)}}

WITH
    seller_source AS (
        SELECT DISTINCT seller_id
        FROM {{ source('slv_data', 'slv_tb_order_items') }}
)

SELECT md5(ss.seller_id) seller_key,
       ss.seller_id
FROM seller_source ss