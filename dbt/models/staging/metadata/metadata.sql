{{
    config(
        materialized='view',
        alias='metadata'
    )
}}

SELECT
    m.asin,
    m.categories,
    m.description,
    m.title,
    m.price,
    m.salesRank as sales_rank,
    m.imUrl as im_url,
    m.related,
    m.created_at,
FROM
    {{ source('default', 'staging_metadata') }} AS m;