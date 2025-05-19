{{ config(
    materialized='table',
    alias='dim_product_category',
    post_hook=[
        'ALTER TABLE gld_data.dim_product_category ADD PRIMARY KEY (product_category_key)'
    ]
)}}

WITH
    product_category AS (
        SELECT DISTINCT coalesce(stp.product_category_name, 'unknown') product_category_name
        FROM {{ source('slv_data', 'slv_tb_products') }} stp
    )
SELECT md5(pc.product_category_name) product_category_key,
       pc.product_category_name
FROM product_category pc