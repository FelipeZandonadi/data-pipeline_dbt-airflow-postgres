{{ config(
    materialized='table', 
    alias='dim_product',
    post_hook=[
        'ALTER TABLE gld_data.dim_product ADD PRIMARY KEY (id_product)',
        'ALTER TABLE gld_data.dim_product ADD CONSTRAINT fk_key_product_category FOREIGN KEY (key_product_category) REFERENCES gld_data.dim_product_category(id_product_category_sk)'
    ]
)}}

WITH
    source AS (
        SELECT *
        FROM {{ source('slv_data', 'slv_tb_products') }} stp
    )

SELECT stp.product_id id_product,
       md5(stp.product_category_name) key_product_category,
       stp.product_weight_g weight_g,
       stp.product_length_cm length_cm,
       stp.product_height_cm height_cm,
       stp.product_width_cm width_cm
FROM source stp
