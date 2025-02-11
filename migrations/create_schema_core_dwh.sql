CREATE  TABLE IF NOT EXISTS default.core_dwh_dim_product (
    asin        VARCHAR,
    description TEXT,
    title       VARCHAR,
    price       NUMERIC,
    imUrl       VARCHAR
)
ENGINE MergeTree() 
ORDER BY asin;

CREATE TABLE IF NOT EXISTS default.core_dwh_dim_category (
    category_id   UInt64,
    category_name VARCHAR
)
ENGINE MergeTree() 
ORDER BY category_id;;

CREATE TABLE IF NOT EXISTS default.core_dwh_fact_sales_rank (
    product_asin VARCHAR,
    category_id  INT,
    rank         INT
)
ENGINE MergeTree() 
ORDER BY product_asin;

CREATE TABLE IF NOT EXISTS default.core_dwh_fact_product_category (
    product_asin VARCHAR,
    category_id  INT
)
ENGINE MergeTree() 
ORDER BY product_asin;

CREATE TABLE IF NOT EXISTS default.core_dwh_fact_product_related_product (
    product_asin VARCHAR,
    related_asin VARCHAR
)
ENGINE MergeTree() 
ORDER BY related_asin;

CREATE TABLE IF NOT EXISTS default.core_dwh_fact_rating (
    reviewerID         VARCHAR(128),
    item            VARCHAR(32),
    rating          NUMERIC,
    event_timestamp TIMESTAMP
)
ENGINE MergeTree() 
ORDER BY event_timestamp;
