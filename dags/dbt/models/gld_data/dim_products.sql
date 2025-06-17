{{ config(
    materialized='table', 
    alias='dim_products',
    post_hook=[
        'ALTER TABLE gld_data.dim_products ADD PRIMARY KEY (CD_product)',
        'ALTER TABLE gld_data.dim_products ADD CONSTRAINT fk_key_product_category FOREIGN KEY (SK_product_category) REFERENCES gld_data.dim_product_categories(SK_product_category)'
    ]
)}}

WITH
    source AS (
        SELECT *
        FROM {{ source("slv_data", "slv_tb_products") }}
    )
SELECT s.cd_product AS CD_product,
       md5(s.nm_category) AS SK_product_category,
       s.vl_weight_g AS VL_weight_g,
       s.vl_length_cm AS VL_length_cm,
       s.vl_height_cm AS VL_height_cm,
       s.vl_width_cm AS VL_width_cm
FROM source s
