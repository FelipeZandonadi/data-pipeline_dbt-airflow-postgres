{{
    config(
        materialized='table'
    )
}}

SELECT *
FROM {{ source('raw', 'tb_orders') }}
