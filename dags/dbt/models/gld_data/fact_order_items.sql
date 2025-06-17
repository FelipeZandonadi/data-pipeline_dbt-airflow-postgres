{{ config(
    materialized='table',
    alias='fact_order_items',
    post_hook=[
        'ALTER TABLE gld_data.fact_order_items ADD PRIMARY KEY (SK_order_item)',
        'ALTER TABLE gld_data.fact_order_items ADD CONSTRAINT fk_id_product FOREIGN KEY (CD_product) REFERENCES gld_data.dim_products(CD_product)',
        'ALTER TABLE gld_data.fact_order_items ADD CONSTRAINT fk_id_seller FOREIGN KEY (CD_seller) REFERENCES gld_data.agr_sellers(CD_seller)',
        ]
)}}


WITH
    source AS (
        select *
        from {{ source("slv_data", "slv_tb_order_items") }}
    )
SELECT
    stoi.cd_order || '_' || stoi.nr_item AS SK_order_item,
    stoi.cd_product AS CD_product,
    stoi.cd_seller AS CD_seller,
    CAST(stoi.dh_shipping_limit AS DATE) AS DT_shipping_limit,
    stoi.vl_price AS VL_price,
    stoi.vl_freight AS VL_shipping
FROM source stoi