{{ config(
    materialized='table',
    alias='fact_order_payments',
    post_hook=[
        'ALTER TABLE gld_data.fact_order_payments ADD PRIMARY KEY (SK_order_payment)',
        'ALTER TABLE gld_data.fact_order_payments ADD CONSTRAINT fk_id_customer FOREIGN KEY (CD_customer) REFERENCES gld_data.agr_customers(CD_customer)',
        'ALTER TABLE gld_data.fact_order_payments ADD CONSTRAINT fk_key_purchase_date FOREIGN KEY (DT_purchase) REFERENCES gld_data.dim_dates(SK_date)',
        'ALTER TABLE gld_data.fact_order_payments ADD CONSTRAINT fk_key_approved_date FOREIGN KEY (DT_approved) REFERENCES gld_data.dim_dates(SK_date)',
        'ALTER TABLE gld_data.fact_order_payments ADD CONSTRAINT fk_key_delivered_carrier_date FOREIGN KEY (DT_delivered_carrier) REFERENCES gld_data.dim_dates(SK_date)',
        'ALTER TABLE gld_data.fact_order_payments ADD CONSTRAINT fk_key_delivered_customer_date FOREIGN KEY (DT_delivered_customer) REFERENCES gld_data.dim_dates(SK_date)',
        'ALTER TABLE gld_data.fact_order_payments ADD CONSTRAINT fk_key_estimated_delivery_date FOREIGN KEY (DT_estimated_delivery) REFERENCES gld_data.dim_dates(SK_date)',
    ]
)}}

WITH
    source AS (
        SELECT
            sto.cd_order,
            stop.nr_payment,
            S.cd_customer_unique,
            sto.dh_purchase,
            sto.dh_approved,
            sto.dh_delivered_carrier,
            sto.dh_delivered_customer,
            sto.dh_estimated_delivery,
            sto.fl_status,
            stop.tp_payment,
            stop.qt_installments,
            stop.vl_payment
        FROM {{ source("slv_data", "slv_tb_orders") }} AS sto
        JOIN {{ source("slv_data", "slv_tb_order_payments") }} stop on sto.cd_order = stop.cd_order
        JOIN {{ source("slv_data", "slv_tb_customers") }} s on s.cd_customer = sto.cd_customer
    )


SELECT
    s.cd_order || '-' || s.nr_payment AS SK_order_payment,
    s.cd_customer_unique CD_customer,

    CAST(s.dh_purchase AS DATE) AS DT_purchase,
    CAST(s.dh_approved AS DATE) AS DT_approved,
    CAST(s.dh_delivered_carrier AS DATE) AS DT_delivered_carrier,
    CAST(s.dh_delivered_customer AS DATE) AS DT_delivered_customer,
    CAST(s.dh_estimated_delivery AS DATE) AS DT_estimated_delivery,

    s.fl_status AS FL_order_status,
    s.tp_payment AS TP_payments,
    s.qt_installments AS QT_installments,
    s.vl_payment AS VL_payments
FROM source s
