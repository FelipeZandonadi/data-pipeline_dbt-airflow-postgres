{{ config(
    materialized='table',
    alias='dim_dates',
    post_hook=[
        'ALTER TABLE gld_data.dim_dates ADD PRIMARY KEY (SK_date)',
    ]
) }}


WITH
    dates AS (
        SELECT
            {{ catch_date_information("sto.dh_purchase") }}
        FROM {{ source("slv_data", "slv_tb_orders") }} AS sto
        WHERE sto.dh_purchase IS NOT NULL
        UNION
        SELECT
            {{ catch_date_information("sto.dh_approved") }}
        FROM {{ source("slv_data", "slv_tb_orders") }} AS sto
        WHERE sto.dh_approved IS NOT NULL
        UNION
        SELECT
            {{ catch_date_information("sto.dh_delivered_carrier") }}
        FROM {{ source("slv_data", "slv_tb_orders") }} AS sto
        WHERE sto.dh_delivered_carrier IS NOT NULL
        UNION
        SELECT
            {{ catch_date_information("sto.dh_delivered_customer") }}
        FROM {{ source("slv_data", "slv_tb_orders") }} AS sto
        WHERE sto.dh_delivered_customer IS NOT NULL
        UNION
        SELECT
            {{ catch_date_information("sto.dh_estimated_delivery") }}
        FROM {{ source("slv_data", "slv_tb_orders") }} AS sto
        WHERE sto.dh_estimated_delivery IS NOT NULL
        UNION
        SELECT
            {{ catch_date_information("stor.dh_creation") }}
        FROM {{ source("slv_data", "slv_tb_order_reviews") }} AS stor
        WHERE stor.dh_creation IS NOT NULL
        UNION
        SELECT
            {{ catch_date_information("stor.dh_answer") }}
        FROM {{ source("slv_data", "slv_tb_order_reviews") }} AS stor
        WHERE stor.dh_answer IS NOT NULL
        UNION
        SELECT
            {{ catch_date_information("stoi.dh_shipping_limit") }}
        FROM {{ source("slv_data", "slv_tb_order_items") }} stoi
        WHERE stoi.dh_shipping_limit IS NOT NULL

    )

SELECT
    d.date AS SK_date,
    d.year AS CD_period_year,
    d.month AS CD_period_month,
    d.day AS CD_period_day,
    d.quarter AS CD_period_quarter,
    {{ day_of_week("d.dow") }} AS NM_day_week
FROM dates d
