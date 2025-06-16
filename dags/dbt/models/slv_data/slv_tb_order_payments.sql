{{ config(
    materialized='table', 
    alias='slv_tb_order_payments',
    post_hook=[
        'ALTER TABLE slv_data.slv_tb_order_payments ADD PRIMARY KEY (CD_order, NR_payment)',
        'ALTER TABLE slv_data.slv_tb_order_payments ADD CONSTRAINT fk_order_to_payments FOREIGN KEY (CD_order) REFERENCES slv_data.slv_tb_orders(CD_order)'
    ]
) }}

with source as (
    SELECT *
    FROM {{ source('brz_data', 'brz_tb_order_payments') }} AS btop
)

SELECT
    CAST(s.order_id AS TEXT) AS CD_order,
    CAST(s.payment_sequential AS BIGINT) AS NR_payment,
    CAST(s.payment_type AS TEXT) AS TP_payment,
    CAST(s.payment_installments AS BIGINT) AS QT_installments,
    CAST(s.payment_value AS NUMERIC(12, 2)) AS VL_payment
FROM source AS s