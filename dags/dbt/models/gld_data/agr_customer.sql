{{ config(
    materialized='table',
    alias='agr_customer',
    post_hook=[
        'ALTER TABLE gld_data.agr_customer ADD PRIMARY KEY (id_customer)',
        'ALTER TABLE gld_data.agr_customer ADD CONSTRAINT fk_key_state_city FOREIGN KEY (key_state_city) REFERENCES gld_data.dim_location(id_state_city_sk)',
        ]
)}}

WITH
    ranked_customers AS (
        SELECT
            stc.customer_unique_id,
            stc.customer_city,
            stc.customer_state,
            ROW_NUMBER() OVER (
                PARTITION BY stc.customer_unique_id
                ORDER BY s.order_purchase_timestamp DESC
            ) AS rn
        FROM {{ source('slv_data', 'slv_tb_customers') }} stc
        JOIN {{ source('slv_data', 'slv_tb_orders') }} s
            ON stc.customer_id = s.customer_id
    ),
    customers AS (
        SELECT
            customer_unique_id,
            customer_city,
            customer_state
        FROM ranked_customers
        WHERE rn = 1
    ),
    customer_qty_items AS (
        select
            c.customer_unique_id,
            count(*) qty_items_purchased,
            sum(price) sum_price
        from {{ source('slv_data','slv_tb_order_items') }} i
        join {{ source('slv_data', 'slv_tb_orders') }} o on o.order_id = i.order_id
        join {{ source('slv_data', 'slv_tb_customers') }} c on o.customer_id = c.customer_id
        group by c.customer_unique_id
    ),
    customer_qty_orders AS (
        SELECT stc.customer_unique_id, count(*) qty_orders
        FROM {{ source('slv_data', 'slv_tb_orders') }} sto
        JOIN {{ source('slv_data', 'slv_tb_customers') }} stc on stc.customer_id = sto.customer_id
        GROUP BY stc.customer_unique_id
    )
SELECT
    c.customer_unique_id id_customer,
    c.customer_state || '-' || c.customer_city key_state_city,
    qti.qty_items_purchased qt_items_purchased,
    qto.qty_orders qt_orders,
    qti.sum_price tpv,
    qti.sum_price/qto.qty_orders average_ticket
FROM
    customers c
LEFT JOIN
    customer_qty_items qti ON c.customer_unique_id = qti.customer_unique_id
JOIN
    customer_qty_orders qto ON c.customer_unique_id = qto.customer_unique_id
