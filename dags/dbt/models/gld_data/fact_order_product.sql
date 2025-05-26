{{ config(
    materialized='table',
    alias='fact_order_product',
    post_hook=[
        'ALTER TABLE gld_data.fact_order_product ADD PRIMARY KEY (id_order_product_sk)',
        'ALTER TABLE gld_data.fact_order_product ADD CONSTRAINT fk_id_product FOREIGN KEY (id_product) REFERENCES gld_data.dim_product(id_product)',
        ]
)}}


WITH
    source AS (
        select order_id, product_id, seller_id, shipping_limit_date, price price_item, freight_value, count(*) qty_item
        from slv_data.slv_tb_order_items
        group by order_id, product_id, seller_id, shipping_limit_date, price, freight_value
    )
SELECT
    stoi.order_id || '-' || stoi.product_id id_order_product_sk,
    stoi.order_id id_order,
    stoi.product_id id_product,
    stoi.seller_id id_seller,
    stoi.qty_item,
    stoi.price_item,
    stoi.freight_value
FROM source stoi
ORDER BY qty_item desc

