{{ config(
    materialized='table',
    alias='fact_orders',
    post_hook=[
        'ALTER TABLE gld_data.fact_orders ADD PRIMARY KEY (id_order)',
        'ALTER TABLE gld_data.fact_orders ADD CONSTRAINT fk_id_customer FOREIGN KEY (id_customer) REFERENCES gld_data.agr_customer(id_customer)',
        'ALTER TABLE gld_data.fact_orders ADD CONSTRAINT fk_key_purchase_date FOREIGN KEY (key_purchase_date) REFERENCES gld_data.dim_date(id_date_sk)',
        'ALTER TABLE gld_data.fact_orders ADD CONSTRAINT fk_key_approved_date FOREIGN KEY (key_approved_date) REFERENCES gld_data.dim_date(id_date_sk)',
        'ALTER TABLE gld_data.fact_orders ADD CONSTRAINT fk_key_delivered_carrier_date FOREIGN KEY (key_delivered_carrier_date) REFERENCES gld_data.dim_date(id_date_sk)',
        'ALTER TABLE gld_data.fact_orders ADD CONSTRAINT fk_key_delivered_customer_date FOREIGN KEY (key_delivered_customer_date) REFERENCES gld_data.dim_date(id_date_sk)',
        'ALTER TABLE gld_data.fact_orders ADD CONSTRAINT fk_key_estimated_delivery_date FOREIGN KEY (key_estimated_delivery_date) REFERENCES gld_data.dim_date(id_date_sk)',
    ]
)}}

WITH
    installments_amount AS (
        SELECT order_id, sum(payment_installments) installments_amount
        FROM {{ source('slv_data', 'slv_tb_order_payments') }}
        GROUP BY order_id
    ),
    qt_order_items_and_amount_price AS (
        SELECT order_id, count(*) qt_order_items, sum(price) purhase_price
        FROM {{ source('slv_data', 'slv_tb_order_items') }}
        GROUP BY order_id
    ),
    RankedOrders AS (
    SELECT
        sto.order_id,
        stc.customer_unique_id,
        sto.order_purchase_timestamp,
        ROW_NUMBER() OVER (PARTITION BY stc.customer_unique_id ORDER BY sto.order_purchase_timestamp ASC) as rn
    FROM
        {{ source('slv_data', 'slv_tb_orders') }} sto
    JOIN
        {{ source('slv_data', 'slv_tb_customers') }} stc ON sto.customer_id = stc.customer_id
    ),
    first_order_customer AS (
    SELECT
        order_id
    FROM
        RankedOrders
    WHERE
        rn = 1
    )

SELECT
    sto.order_id id_order,
    stc.customer_unique_id id_customer,

    TO_CHAR(sto.order_purchase_timestamp, 'YYYYMMDD') AS key_purchase_date,
    TO_CHAR(sto.order_approved_at, 'YYYYMMDD') AS key_approved_date,
    TO_CHAR(sto.order_delivered_carrier_date, 'YYYYMMDD') AS key_delivered_carrier_date,
    TO_CHAR(sto.order_delivered_customer_date, 'YYYYMMDD') AS key_delivered_customer_date,
    TO_CHAR(sto.order_estimated_delivery_date, 'YYYYMMDD') AS key_estimated_delivery_date,

    sto.order_status,
    ia.installments_amount AS payments_installments_amount,
    qoiaap.qt_order_items,
    qoiaap.purhase_price AS total_purchase_price,

    CASE
        WHEN foc.order_id IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_customer_first_order

FROM {{ source('slv_data', 'slv_tb_orders') }} sto
LEFT JOIN {{ source('slv_data', 'slv_tb_customers') }} stc ON sto.customer_id = stc.customer_id
LEFT JOIN installments_amount ia ON sto.order_id = ia.order_id
LEFT JOIN qt_order_items_and_amount_price qoiaap ON sto.order_id = qoiaap.order_id
LEFT JOIN first_order_customer foc ON sto.order_id = foc.order_id