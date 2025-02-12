{{
    config(
        materialized='incremental',
        unique_key = [
            'review_id'
        ],
        partition_by = 'toYYYYMM(event_timestamp)',
        alias='rating'
    )
}}

WITH distinct_reviews as (
    SELECT DISTINCT
        r.reviewerID AS reviewerID,
        r.asin AS item,
        r.rating AS rating,
        FROM_UNIXTIME(r.unixReviewTime) AS event_timestamp
    FROM
        {{ source('default','staging_items') }} AS r
) 


SELECT
    generateUUIDv4() AS review_id, 
    dr.reviewerID AS reviewerID,
    dr.item AS item,
    dr.rating AS rating,
    event_timestamp AS event_timestamp
FROM
    distinct_reviews AS dr



-- write with a table with ReplacingMergeTree engine