{{ config(
    materialized='table',
    alias='dim_product_category',
    post_hook=[
        'ALTER TABLE gld_data.dim_product_category ADD PRIMARY KEY (id_product_category_sk)'
    ]
)}}

WITH
    product_category AS (
        SELECT DISTINCT coalesce(stp.product_category_name, 'unknown') product_category_name
        FROM {{ source('slv_data', 'slv_tb_products') }} stp
    )
SELECT md5(pc.product_category_name) id_product_category_sk,
       pc.product_category_name
FROM product_category pc