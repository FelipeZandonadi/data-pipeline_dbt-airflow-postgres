{{ config(
    materialized='table',
    alias='dim_customer',
    post_hook=[
        'ALTER TABLE gld_data.dim_customer ADD PRIMARY KEY (customer_key)',
        'ALTER TABLE gld_data.dim_customer ADD CONSTRAINT fk_customer_to_city FOREIGN KEY (city_key) REFERENCES gld_data.dim_city(city_key)'
    ]
)}}

WITH
    customers_unique_id_source AS (
        SELECT DISTINCT ON (customer_unique_id)
            customer_unique_id,
            customer_city,
            customer_state
        FROM {{ source('slv_data','slv_tb_customers') }}
    )

SELECT
    md5(cs.customer_unique_id) customer_key,
    cs.customer_unique_id,
    md5(cs.customer_city || cs.customer_state) city_key
FROM customers_unique_id_source cs