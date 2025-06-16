{{ config(
    materialized='table',
    alias='slv_tb_order_reviews',
    post_hook=[
        "ALTER TABLE slv_data.slv_tb_order_reviews ADD PRIMARY KEY (CD_review, CD_order)",
        "ALTER TABLE slv_data.slv_tb_order_reviews ADD CONSTRAINT fk_order_to_reviews FOREIGN KEY (CD_order) REFERENCES slv_data.slv_tb_orders(CD_order)",
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
    CAST(s.review_id AS TEXT) AS CD_review,
    CAST(s.order_id AS TEXT) AS CD_order,
    CAST(s.review_answer_timestamp AS TIMESTAMP) AS DH_answer,
    CAST(s.review_creation_date AS TIMESTAMP) AS DH_creation,
    CAST(s.review_score AS INTEGER) AS QT_score,
    CAST(s.review_comment_title AS TEXT) AS DS_comment_title,
    CAST(s.review_comment_message AS TEXT) AS DS_comment_message
FROM source AS s