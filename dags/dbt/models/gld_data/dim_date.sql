{{ config(
    materialized='table',
    alias='dim_date',
    post_hook=[
        'ALTER TABLE gld_data.dim_date ADD PRIMARY KEY (date_key)'
    ]
) }}

WITH
    dates AS (
        SELECT
            extract(YEAR FROM sto.order_purchase_timestamp) AS year_num,
            extract(MONTH FROM sto.order_purchase_timestamp) AS month_num,
            extract(DAY FROM sto.order_purchase_timestamp) AS day_num,
            extract(ISODOW FROM sto.order_purchase_timestamp) AS dow_num,
            extract(QUARTER FROM sto.order_purchase_timestamp) AS quarter_num
        FROM slv_data.slv_tb_orders AS sto
        WHERE sto.order_purchase_timestamp IS NOT NULL
        UNION
        SELECT
            extract(YEAR FROM sto.order_approved_at),
            extract(MONTH FROM sto.order_approved_at),
            extract(DAY FROM sto.order_approved_at),
            extract(ISODOW FROM sto.order_approved_at),
            extract(QUARTER FROM sto.order_approved_at)
        FROM slv_data.slv_tb_orders AS sto
        WHERE sto.order_approved_at IS NOT NULL
        UNION
        SELECT
            extract(YEAR FROM sto.order_delivered_carrier_date),
            extract(MONTH FROM sto.order_delivered_carrier_date),
            extract(DAY FROM sto.order_delivered_carrier_date),
            extract(ISODOW FROM sto.order_delivered_carrier_date),
            extract(QUARTER FROM sto.order_delivered_carrier_date)
        FROM slv_data.slv_tb_orders AS sto
        WHERE sto.order_delivered_carrier_date IS NOT NULL
        UNION
        SELECT
            extract(YEAR FROM sto.order_delivered_customer_date),
            extract(MONTH FROM sto.order_delivered_customer_date),
            extract(DAY FROM sto.order_delivered_customer_date),
            extract(ISODOW FROM sto.order_delivered_customer_date),
            extract(QUARTER FROM sto.order_delivered_customer_date)
        FROM slv_data.slv_tb_orders AS sto
        WHERE sto.order_delivered_customer_date IS NOT NULL
        UNION
        SELECT
            extract(YEAR FROM sto.order_estimated_delivery_date),
            extract(MONTH FROM sto.order_estimated_delivery_date),
            extract(DAY FROM sto.order_estimated_delivery_date),
            extract(ISODOW FROM sto.order_estimated_delivery_date),
            extract(QUARTER FROM sto.order_estimated_delivery_date)
        FROM slv_data.slv_tb_orders AS sto
        WHERE sto.order_estimated_delivery_date IS NOT NULL
    )

SELECT
    CAST(d.year_num AS text) ||
    CASE
        WHEN length(CAST(d.month_num AS text)) = 1 then '0' || CAST(d.month_num AS text)
        ELSE CAST(d.month_num AS text)
        END ||
    CASE
        WHEN length(CAST(d.day_num AS text)) = 1 THEN '0' || CAST(d.day_num AS text)
        ELSE CAST(d.day_num AS text)
        END AS date_key,
    CAST(d.year_num AS text) || '-' ||
    CASE
        WHEN length(CAST(d.month_num AS text)) = 1 then '0' || CAST(d.month_num AS text)
        ELSE CAST(d.month_num AS text)
        END || '-' ||
    CASE
        WHEN length(CAST(d.day_num AS text)) = 1 THEN '0' || CAST(d.day_num AS text)
        ELSE CAST(d.day_num AS text)
        END AS date_text,
    d.year_num AS year_num,
    d.month_num AS month_num,
    d.day_num AS day_num,
    case
        when d.dow_num = 1 then 'monday'
        when d.dow_num = 2 then 'tuesday'
        when d.dow_num = 3 then 'wednesday'
        when d.dow_num = 4 then 'thursday'
        when d.dow_num = 5 then 'friday'
        when d.dow_num = 6 then 'saturday'
        when d.dow_num = 7 then 'sunday'
        else 'unknown'
    end AS day_week,
    d.quarter_num AS quarter
FROM dates d