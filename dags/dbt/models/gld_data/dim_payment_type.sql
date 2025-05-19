{{ config(
    materialized='table',
    alias='dim_payment_type',
    post_hook=[
        'ALTER TABLE gld_data.dim_payment_type ADD PRIMARY KEY (payment_type_key)'
    ]
)}}

WITH
    payment_type_source AS (
        SELECT DISTINCT coalesce(stop.payment_type, 'not_defined') payment_type
        FROM {{ source('slv_data', 'slv_tb_order_payments') }} stop

    )

SELECT
    md5(ps.payment_type) payment_type_key,
    ps.payment_type
from payment_type_source ps