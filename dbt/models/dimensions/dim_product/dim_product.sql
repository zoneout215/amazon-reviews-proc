{{
    config(
        materialized='incremental',
        unique_key = 'asin',
        alias='dim_product'
    )
}}

SELECT
    mt.asin,
    mt.description,
    mt.title,
    mt.price,
    mt.im_url as im_url
FROM
    {{ ref('metadata') }} AS mt
