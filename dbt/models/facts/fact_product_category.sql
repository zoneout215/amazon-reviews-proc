{{
    config(
        materialized='table',
        schema='amazon_reviews_dbt'
    )
}}

WITH product_categories AS (
    SELECT 
        asin AS asin, 
        categories AS category_name     
    FROM
        {{ ref('stg_metadata') }}
)

SELECT
    DISTINCT
    pc.asin AS product_asin,
    dim_cat.category_id AS category_id
FROM
    product_categories AS pc
    INNER JOIN {{ ref('dim_category') }} AS dim_cat 
    ON dim_cat.category_name = pc.category_name