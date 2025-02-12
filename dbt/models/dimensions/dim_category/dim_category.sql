{{
    config(
        materialized='incremental',
        unique_key = 'category_name',
        alias='dim_category'
    )
}}


With category_elements AS (
    SELECT 
        DISTINCT
        arrayJoin(
            arrayJoin(
                mt.categories
                )) as category_name
    FROM  {{ ref('metadata') }} AS mt
)

SELECT
    DISTINCT
    generateUUIDv4()    AS category_id,
    ce.category_name     AS category_name

FROM
    category_elements AS ce
