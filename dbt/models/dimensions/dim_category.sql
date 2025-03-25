{{
    config(
        materialized='table',
        schema='amazon_reviews_dbt'
    )
}}

WITH category_elements AS (
    SELECT 
        DISTINCT
        categories AS category_name
    FROM
        {{ ref('stg_metadata') }}
)
SELECT
    DISTINCT
    GENERATE_UUID() AS category_id,
    ce.category_name AS category_name
FROM
    category_elements AS ce