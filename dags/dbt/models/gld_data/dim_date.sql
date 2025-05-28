{{ config(
    materialized='table',
    alias='dim_date',
    post_hook=[
        'ALTER TABLE gld_data.dim_date ADD PRIMARY KEY (id_date_sk)',
    ]
) }}

WITH
    dates AS (
        SELECT
            {{ catch_date_information('sto.order_purchase_timestamp') }}
        FROM {{ source('slv_data', 'slv_tb_orders') }} AS sto
        WHERE sto.order_purchase_timestamp IS NOT NULL
        UNION
        SELECT
            {{ catch_date_information('sto.order_approved_at') }}
        FROM {{ source('slv_data', 'slv_tb_orders') }} AS sto
        WHERE sto.order_approved_at IS NOT NULL
        UNION
        SELECT
            {{ catch_date_information('sto.order_delivered_carrier_date') }}
        FROM {{ source('slv_data', 'slv_tb_orders') }} AS sto
        WHERE sto.order_delivered_carrier_date IS NOT NULL
        UNION
        SELECT
            {{ catch_date_information('sto.order_delivered_customer_date') }}
        FROM {{ source('slv_data', 'slv_tb_orders') }} AS sto
        WHERE sto.order_delivered_customer_date IS NOT NULL
        UNION
        SELECT
            {{ catch_date_information('sto.order_estimated_delivery_date') }}
        FROM {{ source('slv_data', 'slv_tb_orders') }} AS sto
        WHERE sto.order_estimated_delivery_date IS NOT NULL
        UNION
        SELECT
            {{ catch_date_information('stor.review_creation_date') }}
        FROM {{ source('slv_data', 'slv_tb_order_reviews') }} AS stor
        WHERE stor.review_creation_date IS NOT NULL
        UNION
        SELECT
            {{ catch_date_information('stor.review_answer_timestamp') }}
        FROM {{ source('slv_data', 'slv_tb_order_reviews') }} AS stor
        WHERE stor.review_answer_timestamp IS NOT NULL

    )

SELECT
    CAST(d.year AS text) ||
    CASE
        WHEN length(CAST(d.month AS text)) = 1 then '0' || CAST(d.month AS text)
        ELSE CAST(d.month AS text)
        END ||
    CASE
        WHEN length(CAST(d.day AS text)) = 1 THEN '0' || CAST(d.day AS text)
        ELSE CAST(d.day AS text)
        END AS id_date_sk,
    d.date AS date,
    d.year AS year,
    d.month AS month,
    d.day AS day,
    {{ day_of_week('d.day_week') }} AS day_week,
    d.quarter AS quarter
FROM dates d