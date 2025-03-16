CREATE OR REPLACE VIEW `amazon_reviews.staging_metadata` AS
SELECT
    JSON_EXTRACT_SCALAR(m.json_string, '$.asin') AS asin,
    category AS categories,
    JSON_EXTRACT_SCALAR(m.json_string, '$.description') AS description,
    JSON_EXTRACT_SCALAR(m.json_string, '$.title') AS title,
    SAFE_CAST(JSON_EXTRACT_SCALAR(m.json_string, '$.price') AS FLOAT64) AS price,
    SAFE_CAST(JSON_EXTRACT_SCALAR(m.json_string, '$.salesRank') AS INT64) AS salesRank,
    JSON_EXTRACT_SCALAR(m.json_string, '$.imUrl') AS imUrl,
    JSON_EXTRACT_SCALAR(m.json_string, '$.related') AS related,
    created_at
FROM `amazon_reviews.raw_data_metadata` AS m,
     UNNEST(JSON_EXTRACT_ARRAY(m.json_string, '$.categories')) AS nested,
     UNNEST(JSON_EXTRACT_ARRAY(nested)) AS category;

CREATE OR REPLACE VIEW `amazon_reviews.staging_items` AS
SELECT
    JSON_EXTRACT_SCALAR(r.json_string, '$.reviewerID') AS reviewerID,
    JSON_EXTRACT_SCALAR(r.json_string, '$.asin') AS asin,
    JSON_EXTRACT_SCALAR(r.json_string, '$.reviewerName') AS reviewerName,
    SAFE_CAST(JSON_EXTRACT_SCALAR(r.json_string, '$.helpful') AS INT64) AS helpful,
    JSON_EXTRACT_SCALAR(r.json_string, '$.reviewText') AS reviewText,
    SAFE_CAST(JSON_EXTRACT_SCALAR(r.json_string, '$.overall') AS FLOAT64) AS rating,
    JSON_EXTRACT_SCALAR(r.json_string, '$.summary') AS summary,
    SAFE_CAST(JSON_EXTRACT_SCALAR(r.json_string, '$.unixReviewTime') AS INT64) AS unixReviewTime,
    JSON_EXTRACT_SCALAR(r.json_string, '$.reviewTime') AS reviewTime,
    created_at
FROM `amazon_reviews.raw_data_items` AS r;