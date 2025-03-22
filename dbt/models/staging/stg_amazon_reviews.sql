{{
    config(
        materialized='view',
        schema='amazon_reviews_dbt'
    )
}}

SELECT DISTINCT 
    reviewerID,
    asin,
    overall,
    unixReviewTime
FROM {{ source('amazon_reviews_dbt', 'gcs_raw_items') }}