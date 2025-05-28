{{ config(
    materialized='table',
    alias='agr_sellers',
    post_hook=[
        'ALTER TABLE gld_data.agr_sellers ADD PRIMARY KEY (id_seller)',
        'ALTER TABLE gld_data.agr_sellers ADD CONSTRAINT fk_key_state_city FOREIGN KEY (key_state_city) REFERENCES gld_data.dim_location(id_state_city_sk)',
    ]
) }}

WITH
    sales_amount_and_sold_item_qt AS (
        SELECT seller_id id_seller, sum(price) sales_amount, count(*) sold_item_qt
        FROM {{ source('slv_data', 'slv_tb_order_items') }}
        GROUP BY seller_id
    ),
    per_seller_per_order AS (
        SELECT seller_id id_seller, order_id id_order
        FROM {{ source('slv_data', 'slv_tb_order_items') }}
        GROUP BY seller_id, order_id
    ),
    sold_order_count AS (
        SELECT id_seller, count(*) count
        FROM per_seller_per_order
        GROUP BY id_seller
    ),
    per_seller_per_customer AS (
        SELECT sts.seller_id id_seller, stc.customer_unique_id
        FROM {{ source('slv_data', 'slv_tb_sellers') }} sts
        JOIN {{ source('slv_data', 'slv_tb_order_items') }} stoi on sts.seller_id = stoi.seller_id
        JOIN {{ source('slv_data', 'slv_tb_orders') }} sto on sto.order_id = stoi.order_id
        JOIN {{ source('slv_data', 'slv_tb_customers') }} stc on stc.customer_id = sto.customer_id
        GROUP BY sts.seller_id, stc.customer_unique_id
    ),
    unique_customer_qt AS (
        SELECT id_seller, count(*) unique_customer_qt
        FROM per_seller_per_customer
        GROUP BY id_seller
    )
SELECT
    sts.seller_id id_seller,
    sts.seller_state || '-' || sts.seller_city key_state_city,
    soc.count total_sold_order_qt,
    saasiq.sold_item_qt total_sold_item_qt,
    ucq.unique_customer_qt total_unique_customer_qt,
    saasiq.sales_amount total_sales_amount
FROM {{ source('slv_data', 'slv_tb_sellers') }} sts
JOIN sales_amount_and_sold_item_qt saasiq ON sts.seller_id = saasiq.id_seller
JOIN sold_order_count soc ON sts.seller_id = soc.id_seller
JOIN unique_customer_qt ucq ON sts.seller_id = ucq.id_seller