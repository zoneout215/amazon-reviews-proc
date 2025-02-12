{{
    config(
        materialized='ephemeral',
        alias='temp_prp_bought'
    )
}}

SELECT
    bav.asin                            AS product_asin,
    arrayJoin(
        JSONExtract(bav.related, 'buy_after_viewing', 'Array(String)')
    ) AS related_asin,
    'buy_after_viewing'                                       AS label
FROM
    {{ ref('metadata') }} AS bav

WHERE
   arrayJoin(
        JSONExtract(bav.related, 'buy_after_viewing', 'Array(String)')
    ) IS NOT NULL

