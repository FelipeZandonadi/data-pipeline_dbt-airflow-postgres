{{ config(
    materialized='table', 
    alias='slv_tb_order_payments',
    post_hook=[
        'ALTER TABLE slv_data.slv_tb_order_payments ADD PRIMARY KEY (order_id, payment_sequential)',
        'ALTER TABLE slv_data.slv_tb_order_payments ADD CONSTRAINT fk_order_to_payments FOREIGN KEY (order_id) REFERENCES slv_data.slv_tb_orders(order_id)'
    ]
) }}

with source as (
    SELECT *
    FROM {{ source('brz_data', 'brz_tb_order_payments') }} AS btop
)

SELECT
    CAST(s.order_id AS TEXT) AS order_id,
    CAST(s.payment_sequential AS BIGINT) AS payment_sequential,
    CAST(s.payment_type AS TEXT) AS payment_type,
    CAST(s.payment_installments AS BIGINT) AS payment_installments,
    CAST(s.payment_value AS NUMERIC(10, 2)) AS payment_value
FROM source AS s