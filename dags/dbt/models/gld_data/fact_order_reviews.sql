{{ config(
    materialized='table',
    alias='fact_order_reviews',
    post_hook=[
        'ALTER TABLE gld_data.fact_order_reviews ADD PRIMARY KEY (SK_order_review)',
        'ALTER TABLE gld_data.fact_order_reviews ADD CONSTRAINT fk_sk_date_answer FOREIGN KEY (DT_answer) REFERENCES gld_data.dim_dates(SK_date)',
        'ALTER TABLE gld_data.fact_order_reviews ADD CONSTRAINT fk_sk_date_creation FOREIGN KEY (DT_creation) REFERENCES gld_data.dim_dates(SK_date)',
    ]
) }}


WITH source AS (
    SELECT *
    FROM {{ source("slv_data", "slv_tb_order_reviews") }}
)
SELECT
    s.cd_order || '-' || s.cd_review AS SK_order_review,
    CAST(s.dh_answer AS DATE) AS DT_answer,
    CAST(s.dh_creation AS DATE) AS DT_creation,
    s.qt_score AS QT_score,
    s.ds_comment_title AS DS_comment_title,
    S.ds_comment_message AS DS_comment_message
FROM source s
