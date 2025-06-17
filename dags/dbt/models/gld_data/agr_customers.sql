{{ config(
    materialized='table',
    alias='agr_customers',
    post_hook=[
        'ALTER TABLE gld_data.agr_customers ADD PRIMARY KEY (CD_customer)',
        'ALTER TABLE gld_data.agr_customers ADD CONSTRAINT fk_key_state_city FOREIGN KEY (SK_state_city) REFERENCES gld_data.dim_locations(SK_state_city)',
        ]
)}}

WITH
    ranked_customers AS (
        SELECT
            stc.cd_customer_unique,
            stc.nm_city,
            stc.nm_state,
            ROW_NUMBER() OVER (
                PARTITION BY stc.cd_customer_unique
                ORDER BY s.dh_purchase DESC
            ) AS rn
        FROM {{ source("slv_data", "slv_tb_customers") }} stc
        LEFT JOIN {{ source("slv_data", "slv_tb_orders") }} s
            ON stc.cd_customer = s.cd_customer
    ),
    customers AS (
        SELECT
            cd_customer_unique,
            nm_city,
            nm_state
        FROM ranked_customers
        WHERE rn = 1
    ),
    customer_qty_items AS (
        select
            c.cd_customer_unique,
            count(*) qty_items_purchased,
            sum(vl_price) sum_price
        from {{ source("slv_data", "slv_tb_order_items") }} i
        join {{ source("slv_data", "slv_tb_orders") }} o on o.cd_order = i.cd_order
        join {{ source("slv_data", "slv_tb_customers") }} c on o.cd_customer = c.cd_customer
        group by c.cd_customer_unique
    ),
    customer_qty_orders AS (
        SELECT stc.cd_customer_unique, count(*) qty_orders
        FROM {{ source("slv_data", "slv_tb_orders") }} sto
        JOIN {{ source("slv_data", "slv_tb_customers") }} stc on stc.cd_customer = sto.cd_customer
        GROUP BY stc.cd_customer_unique
    )
SELECT
    c.cd_customer_unique AS CD_customer,
    c.nm_state || '-' || c.nm_city SK_state_city,
    qto.qty_orders AS QT_orders,
    qti.qty_items_purchased AS QT_items_purchased,
    qti.sum_price AS VL_tpv,
    qti.sum_price/qto.qty_orders AS VL_avg_ticket_order,
    qti.sum_price/qti.qty_items_purchased AS VL_avg_ticket_item
FROM
    customers c
LEFT JOIN
    customer_qty_items qti ON c.cd_customer_unique = qti.cd_customer_unique
JOIN
    customer_qty_orders qto ON c.cd_customer_unique = qto.cd_customer_unique