{{ config(
    materialized='table',
    alias='fact_customer',
    post_hook=[
        'ALTER TABLE gld_data.fact_customer ADD PRIMARY KEY (id_customer_sk)',
        'ALTER TABLE gld_data.fact_customer ADD CONSTRAINT fk_key_state_city FOREIGN KEY (key_state_city) REFERENCES gld_data.dim_location(id_state_city_sk)',
        'ALTER TABLE gld_data.fact_customer ADD CONSTRAINT fk_key_last_order_date FOREIGN KEY (key_last_order_date) REFERENCES gld_data.dim_date(id_date_sk)',
        ]
)}}

WITH ranked_orders AS (
        SELECT
            sto.order_id,
            stc.customer_unique_id,
            sto.order_approved_at,
            ROW_NUMBER() OVER (
                PARTITION BY stc.customer_unique_id
                ORDER BY sto.order_approved_at DESC
            ) AS rn
        FROM slv_data.slv_tb_orders sto
        JOIN slv_data.slv_tb_customers stc
            ON stc.customer_id = sto.customer_id
),
    last_order_customer AS (
        SELECT
            order_id,
            customer_unique_id,
            order_approved_at AS last_order_date
        FROM
            ranked_orders
        WHERE
            rn = 1
),
    ranked_customers AS (
        SELECT
            stc.customer_unique_id,
            stc.customer_city,
            stc.customer_state,
            s.order_purchase_timestamp,
            ROW_NUMBER() OVER (
                PARTITION BY stc.customer_unique_id
                ORDER BY s.order_purchase_timestamp DESC
            ) AS rn
        FROM slv_data.slv_tb_customers stc
        JOIN slv_data.slv_tb_orders s
            ON stc.customer_id = s.customer_id
),
    customers AS (
        SELECT
            customer_unique_id,
            customer_city,
            customer_state,
            order_purchase_timestamp
        FROM ranked_customers
        WHERE rn = 1
),
    customer_qty_items AS (
        select
            c.customer_unique_id,
            count(*) qty_items_purchased,
            sum(price) sum_price
        from slv_data.slv_tb_order_items i
        join slv_data.slv_tb_orders o on o.order_id = i.order_id
        join slv_data.slv_tb_customers c on o.customer_id = c.customer_id
        group by c.customer_unique_id
),
    customer_qty_orders AS (
        SELECT stc.customer_unique_id, count(*) qty_orders
        FROM slv_data.slv_tb_orders sto
        JOIN slv_data.slv_tb_customers stc on stc.customer_id = sto.customer_id
        GROUP BY stc.customer_unique_id
    )
SELECT
    c.customer_unique_id id_customer_sk,
    c.customer_state || '-' || c.customer_city key_state_city,
    to_char(loc.last_order_date, 'YYYYMMDD') key_last_order_date,
    qti.qty_items_purchased,
    qto.qty_orders,
    sum_price amount_payment,
    loc.order_id last_id_order
FROM
    customers c
LEFT JOIN
    customer_qty_items qti ON c.customer_unique_id = qti.customer_unique_id
JOIN
    customer_qty_orders qto ON c.customer_unique_id = qto.customer_unique_id
JOIN
    last_order_customer loc ON c.customer_unique_id = loc.customer_unique_id

