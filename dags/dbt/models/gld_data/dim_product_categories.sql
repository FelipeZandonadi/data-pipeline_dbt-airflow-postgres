{{ config(
    materialized='table',
    alias='dim_product_categories',
    post_hook=[
        'ALTER TABLE gld_data.dim_product_categories ADD PRIMARY KEY (SK_product_category)'
    ]
)}}

WITH
    product_category AS (
        SELECT DISTINCT coalesce(stp.nm_category, 'unknown') product_category_name
        FROM {{ source("slv_data", "slv_tb_products") }} stp
    )
SELECT md5(pc.product_category_name) SK_product_category,
       pc.product_category_name AS NM_category
FROM product_category pc