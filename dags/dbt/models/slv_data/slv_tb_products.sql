{{ config(
    materialized='table',
    alias='slv_tb_products',
    post_hook='ALTER TABLE slv_data.slv_tb_products ADD PRIMARY KEY (product_id)'
) }}

with source as (
    SELECT
        product_id,
        COALESCE(btp.product_category_name, 'outros') AS product_category_name,
        COALESCE(btp.product_name_lenght, 0) AS product_name_length, -- perceba que o nome da coluna original possui um erro de digitação
        COALESCE(btp.product_description_lenght, 0) AS product_description_length, -- perceba que o nome da coluna original possui um erro de digitação
        COALESCE(btp.product_photos_qty, 0) AS product_photos_qty,
        COALESCE(btp.product_weight_g, 0) AS product_weight_g,
        COALESCE(btp.product_length_cm, 0) AS product_length_cm,
        COALESCE(btp.product_height_cm, 0) AS product_height_cm,
        COALESCE(btp.product_width_cm, 0) AS product_width_cm
    FROM {{ source('brz_data', 'brz_tb_products') }} AS btp
)


SELECT 
    CAST(s.product_id AS TEXT) AS product_id,
    CAST(s.product_category_name AS TEXT) AS product_category_name,
    CAST(s.product_name_length AS INTEGER) AS product_name_length,
    CAST(s.product_description_length AS INTEGER) AS product_description_length,
    CAST(s.product_photos_qty AS INTEGER) AS product_photos_qty,
    CAST(s.product_weight_g AS INTEGER) AS product_weight_g,
    CAST(s.product_length_cm AS INTEGER) AS product_length_cm,
    CAST(s.product_height_cm AS INTEGER) AS product_height_cm,
    CAST(s.product_width_cm AS INTEGER) AS product_width_cm
FROM source AS s