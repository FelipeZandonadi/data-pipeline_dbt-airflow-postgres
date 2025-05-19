{{ config(
    materialized='table',
    alias='slv_tb_orders',
    post_hook=[
        'ALTER TABLE slv_data.slv_tb_orders ADD PRIMARY KEY (order_id)',
        'ALTER TABLE slv_data.slv_tb_orders ADD CONSTRAINT fk_order_to_customer FOREIGN KEY (customer_id) REFERENCES slv_data.slv_tb_customers(customer_id)'
    ]
) }}

with source as (
    SELECT bto.order_id,
            bto.customer_id,
            bto.order_status,
            bto.order_purchase_timestamp,
            bto.order_approved_at,
            bto.order_delivered_carrier_date,
            bto.order_delivered_customer_date,
            bto.order_estimated_delivery_date
    FROM {{ source('brz_data', 'brz_tb_orders') }} AS bto
)

SELECT
    CAST(s.order_id AS TEXT) AS order_id,
    CAST(s.customer_id AS TEXT) AS customer_id,
    CAST(s.order_status AS TEXT) AS order_status,
    CAST(s.order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp,
    CAST(s.order_approved_at AS TIMESTAMP) AS order_approved_at,
    CAST(s.order_delivered_carrier_date AS TIMESTAMP) AS order_delivered_carrier_date,
    CAST(s.order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_date,
    CAST(s.order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_date
FROM source AS s