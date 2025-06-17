{{ config(
    materialized='table',
    alias='slv_tb_orders',
    post_hook=[
        'ALTER TABLE slv_data.slv_tb_orders ADD PRIMARY KEY (CD_order)',
        'ALTER TABLE slv_data.slv_tb_orders ADD CONSTRAINT fk_order_to_customer FOREIGN KEY (CD_customer) REFERENCES slv_data.slv_tb_customers(CD_customer)'
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
    CAST(s.order_id AS TEXT) AS CD_order,
    CAST(s.customer_id AS TEXT) AS CD_customer,
    CAST(s.order_purchase_timestamp AS TIMESTAMP) AS DH_purchase,
    CAST(s.order_approved_at AS TIMESTAMP) AS DH_approved,
    CAST(s.order_delivered_carrier_date AS TIMESTAMP) AS DH_delivered_carrier,
    CAST(s.order_delivered_customer_date AS TIMESTAMP) AS DH_delivered_customer,
    CAST(s.order_estimated_delivery_date AS TIMESTAMP) AS DH_estimated_delivery,
    CAST(s.order_status AS TEXT) AS FL_status
FROM source AS s