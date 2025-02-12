{{
    config(
        materialized='view',
        alias='items'
    )
}}

SELECT
    r.reviewerID AS reviewerID,
    r.asin AS asin,
    r.reviewerName AS reviewerName,
    r.helpful AS helpful,
    r.reviewText AS reviewText,
    r.rating AS overall,
    r.summary AS summary,
    r.unixReviewTime AS unixReviewTime,
    r.reviewTime AS reviewTime,
    r.created_at AS created_at
FROM
    {{ source('default', 'staging_items') }} AS r
