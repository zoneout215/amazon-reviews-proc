-- Converted to BigQuery DDL from ClickHouse
CREATE OR REPLACE TABLE `amazon_reviews.dm_rating_top_bottom_five` (
    label      STRING,
    movie      STRING,
    year_month STRING
);

CREATE OR REPLACE TABLE `amazon_reviews.dm_rating_top_five_increased_rating` (
    label      STRING,
    movie      STRING,
    year_month STRING
);
