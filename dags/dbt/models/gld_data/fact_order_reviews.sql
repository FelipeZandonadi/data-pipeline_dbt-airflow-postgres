{{ config(
    materialized='table',
    alias='fact_order_reviews',
    post_hook=[
        'ALTER TABLE gld_data.fact_order_reviews ADD PRIMARY KEY (id_review_order_sk)',
        'ALTER TABLE gld_data.fact_order_reviews ADD CONSTRAINT fk_id_order FOREIGN KEY (id_order) REFERENCES gld_data.fact_orders(id_order)',
        'ALTER TABLE gld_data.fact_order_reviews ADD CONSTRAINT fk_id_date FOREIGN KEY (key_creation_date) REFERENCES gld_data.dim_date(id_date_sk)',
        'ALTER TABLE gld_data.fact_order_reviews ADD CONSTRAINT fk_id_date_answer FOREIGN KEY (key_answer_date) REFERENCES gld_data.dim_date(id_date_sk)',
    ]
) }}


WITH source AS (
    SELECT *
    FROM slv_data.slv_tb_order_reviews
)
SELECT
    s.review_id || '-' || s.order_id AS id_review_order_sk,
    S.review_id AS  id_review,
    s.order_id AS id_order,
    to_char(CAST(s.review_creation_date AS DATE), 'YYYYMMDD') AS key_creation_date,
    to_char(CAST(s.review_answer_timestamp AS DATE), 'YYYYMMDD') AS key_answer_date,
    s.review_score AS score,
    s.review_comment_title AS comment_title,
    S.review_comment_message AS comment_message
FROM source s