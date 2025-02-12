{{
    config(
        materialized='incremental',
        unique_key = [
            'product_asin',
            'category_id'
        ],
        alias='fact_product_category'
    )
}}

WITH product_categories AS (
    SELECT 
        asin AS asin, 
        arrayJoin(
            arrayJoin(
                mt.categories
                )) as category_name     
    FROM
        {{ ref('metadata') }} AS mt
)

SELECT
    DISTINCT
    pc.asin        AS product_asin,
    dim_cat.category_id AS category_id
FROM
    product_categories                   AS pc
    INNER JOIN {{ ref('dim_category') }} AS dim_cat 
    ON dim_cat.category_name = pc.category_name