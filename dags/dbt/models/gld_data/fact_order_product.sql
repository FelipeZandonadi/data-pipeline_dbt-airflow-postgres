{{ config(
    materialized='table',
    alias='fact_order_product',
    post_hook=[
        'ALTER TABLE gld_data.fact_order_product ADD PRIMARY KEY (id_order_product_sk)',
        'ALTER TABLE gld_data.fact_order_product ADD CONSTRAINT fk_id_product FOREIGN KEY (id_product) REFERENCES gld_data.dim_product(id_product)',
        'ALTER TABLE gld_data.fact_order_product ADD CONSTRAINT fk_key_order_approved_at FOREIGN KEY (key_order_approved_at) REFERENCES gld_data.dim_date(id_date_sk)',
        'ALTER TABLE gld_data.fact_order_product ADD CONSTRAINT fk_key_order_delivered_customer_date FOREIGN KEY (key_order_delivered_customer_date) REFERENCES gld_data.dim_date(id_date_sk)',
        'ALTER TABLE gld_data.fact_order_product ADD CONSTRAINT fk_key_order_estimated_delivery_date FOREIGN KEY (key_order_estimated_delivery_date) REFERENCES gld_data.dim_date(id_date_sk)',
        'ALTER TABLE gld_data.fact_order_product ADD CONSTRAINT fk_key_state_city FOREIGN KEY (key_state_city) REFERENCES gld_data.dim_location(id_state_city_sk)',
        ]
)}}


WITH
    source AS (
        select order_id, product_id, seller_id, shipping_limit_date, price, freight_value, count(*) qty_item
        from slv_data.slv_tb_order_items
        group by order_id, product_id, seller_id, shipping_limit_date, price, freight_value
    ),
    order_payments_installments AS (
        select stop.order_id, sum(stop.payment_installments) order_amount_payment_installments
        from slv_data.slv_tb_order_payments stop
        group by stop.order_id
    )
SELECT
    stoi.order_id || '_' || stoi.product_id id_order_product_sk,
    stoi.order_id id_order,
    stoi.product_id id_product,
    stoi.seller_id id_seller,
    TO_CHAR(sto.order_approved_at, 'YYYYMMDD') key_order_approved_at,
    TO_CHAR(sto.order_delivered_customer_date, 'YYYYMMDD') key_order_delivered_customer_date,
    TO_CHAR(sto.order_estimated_delivery_date, 'YYYYMMDD') key_order_estimated_delivery_date,
    stc.customer_state || '_' || stc.customer_city key_state_city,
    sto.order_status,
    stoi.qty_item,
    stoi.price,
    stoi.freight_value,
    opi.order_amount_payment_installments
FROM source stoi
JOIN slv_data.slv_tb_orders sto ON stoi.order_id = sto.order_id
JOIN slv_data.slv_tb_customers stc ON sto.customer_id = stc.customer_id
JOIN order_payments_installments opi ON sto.order_id = opi.order_id
ORDER BY qty_item desc

