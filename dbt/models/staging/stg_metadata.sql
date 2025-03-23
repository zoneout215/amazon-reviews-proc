{{
    config(
        materialized='view',
        schema='amazon_reviews_dbt'
    )
}}

SELECT
    JSON_EXTRACT_SCALAR(m.json_string, '$.asin') AS asin,
    category AS categories,
    JSON_EXTRACT_SCALAR(m.json_string, '$.description') AS description,
    JSON_EXTRACT_SCALAR(m.json_string, '$.title') AS title,
    SAFE_CAST(JSON_EXTRACT_SCALAR(m.json_string, '$.price') AS FLOAT64) AS price,
    SAFE_CAST(JSON_EXTRACT_SCALAR(m.json_string, '$.salesRank') AS INT64) AS salesRank,
    JSON_EXTRACT_SCALAR(m.json_string, '$.imUrl') AS imUrl,
    JSON_EXTRACT_ARRAY(json_string, '$.related.also_bought') AS also_bought_array,
    JSON_EXTRACT_ARRAY(json_string, '$.related.buy_after_viewing') AS buy_after_viewing_array
FROM {{ source('amazon_reviews_dbt', 'gcs_raw_metadata') }} AS m,
     UNNEST(JSON_EXTRACT_ARRAY(m.json_string, '$.categories')) AS nested,
     UNNEST(JSON_EXTRACT_ARRAY(nested)) AS category