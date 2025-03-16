-- Converted to BigQuery DDL from ClickHouse
CREATE OR REPLACE TABLE `amazon_reviews.raw_data_metadata` (
    json_string STRING,
    created_at DATE
);

CREATE OR REPLACE TABLE `amazon_reviews.raw_data_items` (
    json_string STRING,
    created_at DATE
);