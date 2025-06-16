{{ config(
    materialized='table', 
    alias='slv_tb_order_items',
    post_hook=[
        'ALTER TABLE slv_data.slv_tb_order_items ADD PRIMARY KEY (CD_order, NR_item)',
        'ALTER TABLE slv_data.slv_tb_order_items ADD CONSTRAINT fk_order_to_items FOREIGN KEY (CD_order) REFERENCES slv_data.slv_tb_orders(CD_order)',
        'ALTER TABLE slv_data.slv_tb_order_items ADD CONSTRAINT fk_products_to_items FOREIGN KEY (CD_product) REFERENCES slv_data.slv_tb_products(CD_product)',
        'ALTER TABLE slv_data.slv_tb_order_items ADD CONSTRAINT fk_sellers_to_items FOREIGN KEY (CD_seller) REFERENCES slv_data.slv_tb_sellers(CD_seller)',
    ]
) }}

with source as (
    SELECT
        *
    FROM {{ source('brz_data', 'brz_tb_order_items') }} AS btoi
)

SELECT
    CAST(s.order_id AS TEXT) AS CD_order,
    CAST(s.order_item_id AS BIGINT) AS NR_item,
    CAST(s.product_id AS TEXT) AS CD_product,
    CAST(s.seller_id AS TEXT) AS CD_seller,
    CAST(s.shipping_limit_date AS TIMESTAMP) AS DH_shipping_limit,
    CAST(s.price AS NUMERIC(12, 2)) AS VL_price,
    CAST(s.freight_value AS NUMERIC(12, 2)) AS VL_freight
FROM source AS s