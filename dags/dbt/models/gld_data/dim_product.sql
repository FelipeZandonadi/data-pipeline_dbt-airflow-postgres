{{ config(
    materialized='table', 
    alias='dim_product',
    post_hook=[
        'ALTER TABLE gld_data.dim_product ADD PRIMARY KEY (product_key)',
        'ALTER TABLE gld_data.dim_product ADD CONSTRAINT product_category_key FOREIGN KEY (product_category_key) REFERENCES gld_data.dim_product_category(product_category_key)'
    ]
)}}

SELECT md5(stp.product_id) product_key,
       stp.product_id,
       md5(stp.product_category_name) product_category_key,
       stp.product_name_length,
       stp.product_description_length,
       stp.product_photos_qty,
       stp.product_weight_g,
       stp.product_length_cm,
       stp.product_height_cm,
       stp.product_width_cm
FROM {{ source('slv_data', 'slv_tb_products') }} stp
