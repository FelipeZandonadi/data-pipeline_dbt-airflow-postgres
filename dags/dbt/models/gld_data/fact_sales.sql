{{ config(
    materialized='table',
    alias='fact_sales',
    post_hook=[
        'ALTER TABLE gld_data.fact_sales ADD PRIMARY KEY (order_item_key)',
        'ALTER TABLE gld_data.fact_sales ADD CONSTRAINT purchase_date_key FOREIGN KEY (purchase_date_key) REFERENCES gld_data.dim_date(date_key)',
        'ALTER TABLE gld_data.fact_sales ADD CONSTRAINT approved_date_key FOREIGN KEY (approved_date_key) REFERENCES gld_data.dim_date(date_key)',
        'ALTER TABLE gld_data.fact_sales ADD CONSTRAINT delivered_carrier_date_key FOREIGN KEY (delivered_carrier_date_key) REFERENCES gld_data.dim_date(date_key)',
        'ALTER TABLE gld_data.fact_sales ADD CONSTRAINT delivered_customer_date_key FOREIGN KEY (delivered_customer_date_key) REFERENCES gld_data.dim_date(date_key)',
        'ALTER TABLE gld_data.fact_sales ADD CONSTRAINT customer_key FOREIGN KEY (customer_key) REFERENCES gld_data.dim_customer(customer_key)',
        'ALTER TABLE gld_data.fact_sales ADD CONSTRAINT product_key FOREIGN KEY (product_key) REFERENCES gld_data.dim_product(product_key)',
        'ALTER TABLE gld_data.fact_sales ADD CONSTRAINT seller_key FOREIGN KEY (seller_key) REFERENCES gld_data.dim_seller(seller_key)',
        'ALTER TABLE gld_data.fact_sales ADD CONSTRAINT payment_type_key FOREIGN KEY (payment_type_key) REFERENCES gld_data.dim_payment_type(payment_type_key)'
    ]
)}}


SELECT DISTINCT ON (md5(concat(stoi.order_id, stoi.order_item_id)))
    md5(concat(stoi.order_id, stoi.order_item_id)) AS order_item_key,
    stoi.order_id,
    stoi.order_item_id,
    sto.order_status,
    TO_CHAR(sto.order_purchase_timestamp, 'YYYYMMDD') AS purchase_date_key,
    TO_CHAR(sto.order_approved_at, 'YYYYMMDD') AS approved_date_key,
    TO_CHAR(sto.order_delivered_carrier_date, 'YYYYMMDD') AS delivered_carrier_date_key,
    TO_CHAR(sto.order_delivered_customer_date, 'YYYYMMDD') AS delivered_customer_date_key,
    md5(stc.customer_unique_id) customer_key,
    sto.customer_id,
    md5(stoi.product_id) product_key,
    md5(stoi.seller_id) seller_key,
    md5(stop.payment_type) payment_type_key,
    stop.payment_installments,
    stoi.price,
    stoi.freight_value
FROM {{ source('slv_data', 'slv_tb_order_items') }} stoi
JOIN {{ source('slv_data', 'slv_tb_orders') }} sto ON sto.order_id = stoi.order_id
JOIN {{ source('slv_data', 'slv_tb_order_payments') }} stop ON sto.order_id = stop.order_id
JOIN {{ source('slv_data', 'slv_tb_customers') }} stc ON sto.customer_id = stc.customer_id
