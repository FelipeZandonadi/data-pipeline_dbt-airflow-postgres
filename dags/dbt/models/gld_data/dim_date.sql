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
            CAST(sto.order_approved_at AS DATE) AS date,
            extract(YEAR FROM sto.order_approved_at) AS year,
            extract(MONTH FROM sto.order_approved_at) AS month,
            extract(DAY FROM sto.order_approved_at) AS day,
            extract(ISODOW FROM sto.order_approved_at) AS day_of_week,
            extract(QUARTER FROM sto.order_approved_at) AS quarter
        FROM slv_data.slv_tb_orders AS sto
        WHERE sto.order_approved_at IS NOT NULL
        UNION
        SELECT
            CAST(sto.order_delivered_customer_date AS DATE),
            extract(YEAR FROM sto.order_delivered_customer_date),
            extract(MONTH FROM sto.order_delivered_customer_date),
            extract(DAY FROM sto.order_delivered_customer_date),
            extract(ISODOW FROM sto.order_delivered_customer_date),
            extract(QUARTER FROM sto.order_delivered_customer_date)
        FROM slv_data.slv_tb_orders AS sto
        WHERE sto.order_delivered_customer_date IS NOT NULL
        UNION
        SELECT
            CAST(sto.order_estimated_delivery_date AS DATE),
            extract(YEAR FROM sto.order_estimated_delivery_date),
            extract(MONTH FROM sto.order_estimated_delivery_date),
            extract(DAY FROM sto.order_estimated_delivery_date),
            extract(ISODOW FROM sto.order_estimated_delivery_date),
            extract(QUARTER FROM sto.order_estimated_delivery_date)
        FROM slv_data.slv_tb_orders AS sto
        WHERE sto.order_estimated_delivery_date IS NOT NULL
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
    case
        when d.day_of_week = 1 then 'monday'
        when d.day_of_week = 2 then 'tuesday'
        when d.day_of_week = 3 then 'wednesday'
        when d.day_of_week = 4 then 'thursday'
        when d.day_of_week = 5 then 'friday'
        when d.day_of_week = 6 then 'saturday'
        when d.day_of_week = 7 then 'sunday'
        else 'unknown'
    end AS day_of_week,
    d.quarter AS quarter
FROM dates d
