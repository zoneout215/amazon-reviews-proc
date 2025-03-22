-- Create Raw Items GCS table
CREATE OR REPLACE TABLE `e-analogy-449921-p7.amazon_reviews.gcs_raw_items` (
  reviewerName STRING,
  summary STRING,
  reviewText STRING,
  reviewTime STRING,
  asin STRING,
  overall FLOAT64,
  unixReviewTime INT64,
  helpful ARRAY<INT64>,
  reviewerID STRING
);


-- Create Raw Metadata GCS table
CREATE OR REPLACE TABLE `e-analogy-449921-p7.amazon_reviews.gcs_raw_metadata` (
  json_string STRING
);

-- Create Staging Native Metadata table
CREATE OR REPLACE TABLE `e-analogy-449921-p7.amazon_reviews.stag_metadata` (
  asin STRING,
  categories STRING,
  description STRING,
  title STRING,
  price FLOAT64,
  salesRank INT64,
  imUrl STRING,
  related STRING
);

-- Create Reviews table
CREATE OR REPLACE TABLE `e-analogy-449921-p7.amazon_reviews.facts_reviews` (
  reviewerID STRING,
  item STRING,
  rating FLOAT64,
  event_timestamp TIMESTAMP,
  review_id STRING
);

-- Create Categories table
CREATE OR REPLACE TABLE `e-analogy-449921-p7.amazon_reviews.dim_category` (
  category_id STRING,
  category_name STRING
);



-- Create Product Category Fact table
CREATE OR REPLACE TABLE `e-analogy-449921-p7.amazon_reviews.fact_product_category` (
  product_asin STRING,
  category_id STRING
);


-- Resulting table
CREATE OR REPLACE TABLE `e-analogy-449921-p7.amazon_reviews.dm_rating_top_five_catergories` (
  category_name STRING,
  avg_category_rating FLOAT64,
  year_month STRING,
  label STRING
);