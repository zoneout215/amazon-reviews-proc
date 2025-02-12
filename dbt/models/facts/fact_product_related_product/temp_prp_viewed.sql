{{
    config(
        materialized='ephemeral',
        alias='temp_prp_viewed'
    )
}}

SELECT
    av.asin                            AS product_asin,
    arrayJoin(
        JSONExtract(av.related, 'also_viewed', 'Array(String)')
    ) AS related_asin,
    'also_viewed'                                       AS label
FROM
    {{ ref('metadata') }} AS av

WHERE
    arrayJoin(
        JSONExtract(av.related, 'also_viewed', 'Array(String)')
    ) IS NOT NULL
