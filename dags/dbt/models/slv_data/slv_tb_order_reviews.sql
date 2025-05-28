{{ config(
    materialized='table',
    alias='slv_tb_order_reviews',
    post_hook=[
        "ALTER TABLE slv_data.slv_tb_order_reviews ADD PRIMARY KEY (review_id, order_id)",
        "ALTER TABLE slv_data.slv_tb_order_reviews ADD CONSTRAINT fk_order_to_reviews FOREIGN KEY (order_id) REFERENCES slv_data.slv_tb_orders(order_id)",
        ]
) }}

with source as (
    SELECT
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp
    FROM {{ source('brz_data', 'brz_tb_order_reviews') }}
)

SELECT 
    CAST(s.review_id AS TEXT) AS review_id,
    CAST(s.order_id AS TEXT) AS order_id,
    CAST(s.review_score AS INTEGER) AS review_score,
    CAST(s.review_comment_title AS TEXT) AS review_comment_title,
    CAST(s.review_comment_message AS TEXT) AS review_comment_message,
    CAST(s.review_creation_date AS TIMESTAMP) AS review_creation_date,
    CAST(s.review_answer_timestamp AS TIMESTAMP) AS review_answer_timestamp
FROM source AS s