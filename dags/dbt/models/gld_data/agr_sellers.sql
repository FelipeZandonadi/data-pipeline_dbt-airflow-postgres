{{ config(
    materialized='table',
    alias='agr_sellers',
    post_hook=[
        'ALTER TABLE gld_data.agr_sellers ADD PRIMARY KEY (CD_seller)',
        'ALTER TABLE gld_data.agr_sellers ADD CONSTRAINT fk_key_state_city FOREIGN KEY (SK_state_city) REFERENCES gld_data.dim_locations(SK_state_city)',
    ]
) }}

WITH
    sales_amount_and_qt_item AS (
        SELECT cd_seller id_seller, sum(vl_price) sales_amount, count(*) sales_item_qt
        FROM {{ source("slv_data", "slv_tb_order_items") }}
        GROUP BY cd_seller
    ),
    per_seller_per_order AS (
        SELECT cd_seller id_seller, cd_order id_order
        FROM {{ source("slv_data", "slv_tb_order_items") }}
        GROUP BY cd_seller, cd_order
    ),
    sold_order_count AS (
        SELECT id_seller, count(*) count
        FROM per_seller_per_order
        GROUP BY id_seller
    ),
    per_seller_per_customer AS (
        SELECT sts.cd_seller id_seller, stc.cd_customer_unique
        FROM {{ source("slv_data", "slv_tb_sellers") }} sts
        JOIN {{ source("slv_data", "slv_tb_order_items") }} stoi on sts.cd_seller = stoi.cd_seller
        JOIN {{ source("slv_data", "slv_tb_orders") }} sto on sto.cd_order = stoi.cd_order
        JOIN {{ source("slv_data", "slv_tb_customers") }} stc on stc.cd_customer = sto.cd_customer
        GROUP BY sts.cd_seller, stc.cd_customer_unique
    ),
    unique_customer_qt AS (
        SELECT id_seller, count(*) unique_customer_qt
        FROM per_seller_per_customer
        GROUP BY id_seller
    )
SELECT
    sts.cd_seller AS CD_seller,
    sts.nm_state || '-' || sts.nm_city AS SK_state_city,
    soc.count AS QT_orders,
    saasiq.sales_item_qt AS QT_items_sold,
    ucq.unique_customer_qt AS QT_unique_customer,
    saasiq.sales_amount AS VL_gmv,
    saasiq.sales_amount/soc.count VL_avg_ticket_order,
    saasiq.sales_amount/saasiq.sales_item_qt AS VL_avg_ticket_item,
    saasiq.sales_amount/ucq.unique_customer_qt AS VL_avg_ticket_customer
FROM {{ source("slv_data", "slv_tb_sellers") }} sts
JOIN sales_amount_and_qt_item saasiq ON sts.cd_seller = saasiq.id_seller
JOIN sold_order_count soc ON sts.cd_seller = soc.id_seller
JOIN unique_customer_qt ucq ON sts.cd_seller = ucq.id_seller