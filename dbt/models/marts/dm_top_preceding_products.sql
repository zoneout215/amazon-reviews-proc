{{
  config(
    materialized='table',
    schema='amazon_reviews_dbt'
  )
}}

WITH buy_after_viewing_exploded AS (
  -- Explode the buy_after_viewing_array into individual rows
  SELECT
    asin,
    TRIM(JSON_EXTRACT_SCALAR(viewed_product, '$')) AS viewed_after_product
  FROM
    {{ ref('stg_metadata') }},
    UNNEST(buy_after_viewing_array) AS viewed_product
  WHERE
    buy_after_viewing_array IS NOT NULL
),

buy_counts AS (
  -- Count occurrences of each viewed_after_product for each asin
  SELECT
    asin,
    viewed_after_product,
    COUNT(*) AS view_count
  FROM
    buy_after_viewing_exploded
  GROUP BY
    asin,
    viewed_after_product
),

ranked_products AS (
  -- Rank the products by view_count for each asin
  SELECT
    asin,
    viewed_after_product,
    view_count,
    ROW_NUMBER() OVER (PARTITION BY asin ORDER BY view_count DESC) AS rank
  FROM 
    buy_counts
)

-- Aggregate the top 5 products into an array for each asin and add product details
SELECT
  p.asin,
  p.title,
  p.categories,
  p.price,
  ARRAY_AGG(
    STRUCT(viewed_after_product, view_count)
    ORDER BY view_count DESC
    LIMIT 5
  ) AS top_5_buy_after_viewing
FROM
  ranked_products r
JOIN
  {{ ref('stg_metadata') }} p
  ON r.asin = p.asin
WHERE
  rank <= 5
GROUP BY
  p.asin,
  p.title,
  p.categories,
  p.price