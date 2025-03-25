{{
    config(
        materialized='table',
        schema='amazon_reviews_dbt'
    )
}}

SELECT 
    GENERATE_UUID() AS review_id,
    r.reviewerID AS reviewerID,
    r.asin AS item,
    r.overall AS rating,
    TIMESTAMP_SECONDS(r.unixReviewTime) AS event_timestamp
FROM {{ ref('stg_amazon_reviews') }} AS r