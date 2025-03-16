-- Converted to BigQuery DDL from ClickHouse

CREATE OR REPLACE TABLE `amazon_reviews.core_dwh_dim_product` (
    asin        STRING,
    description STRING,
    title       STRING,
    price       NUMERIC,
    imUrl       STRING
);

CREATE OR REPLACE TABLE `amazon_reviews.core_dwh_dim_category` (
    category_id   INT64,
    category_name STRING
);

CREATE OR REPLACE TABLE `amazon_reviews.core_dwh_fact_sales_rank` (
    product_asin STRING,
    category_id  INT64,
    rank         INT64
);

CREATE OR REPLACE TABLE `amazon_reviews.core_dwh_fact_product_category` (
    product_asin STRING,
    category_id  INT64
);

CREATE OR REPLACE TABLE `amazon_reviews.core_dwh_fact_product_related_product` (
    product_asin STRING,
    related_asin STRING
);

CREATE OR REPLACE TABLE `amazon_reviews.core_dwh_fact_rating` (
    reviewerID      STRING,
    item            STRING,
    rating          NUMERIC,
    event_timestamp TIMESTAMP
);
