{{ config(
    materialized='table', 
    alias='slv_tb_order_items',
    post_hook=[
        'ALTER TABLE slv_data.slv_tb_order_items ADD PRIMARY KEY (order_id, order_item_id)',
        'ALTER TABLE slv_data.slv_tb_order_items ADD CONSTRAINT fk_order_to_items FOREIGN KEY (order_id) REFERENCES slv_data.slv_tb_orders(order_id)',
        'ALTER TABLE slv_data.slv_tb_order_items ADD CONSTRAINT fk_products_to_items FOREIGN KEY (product_id) REFERENCES slv_data.slv_tb_products(product_id)'
    ]
) }}

with source as (
    SELECT
        *
    FROM {{ source('brz_data', 'brz_tb_order_items') }} AS btoi
)

SELECT
    CAST(s.order_id AS TEXT) AS order_id,
    CAST(s.order_item_id AS BIGINT) AS order_item_id,
    CAST(s.product_id AS TEXT) AS product_id,
    CAST(s.seller_id AS TEXT) AS seller_id,
    CAST(s.shipping_limit_date AS TIMESTAMP) AS shipping_limit_date,
    CAST(s.price AS NUMERIC(10, 2)) AS price,
    CAST(s.freight_value AS NUMERIC(10, 2)) AS freight_value
FROM source AS s