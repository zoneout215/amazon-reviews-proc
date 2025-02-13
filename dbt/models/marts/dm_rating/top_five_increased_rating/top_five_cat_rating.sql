{{
    config(
        materialized='table',
        alias='dm_rating_top_five_cat_rating',
        pre_hook=[
            "TRUNCATE TABLE {{ this }}"
        ]
    )
}}

WITH reviews_per_product AS (
    SELECT
        r.item AS asin,
        CONCAT(
            EXTRACT(YEAR FROM r.event_timestamp),
            '.',
            EXTRACT(MONTH FROM r.event_timestamp)) AS year_month,
        AVG(r.rating) AS avg_rating,
        Count(DISTINCT(r.review_id)) AS num_ratings
    FROM
        {{ ref('fact_rating') }} AS r
    GROUP BY
        r.item,
        CONCAT(
            EXTRACT(YEAR FROM r.event_timestamp),
            '.',
            EXTRACT(MONTH FROM r.event_timestamp)
        )
),
ranked_categories AS (
    SELECT
        pc.category_id,
        rp.year_month,
        AVG(rp.avg_rating) AS avg_category_rating,
        ROW_NUMBER() OVER (
            PARTITION BY rp.year_month
            ORDER BY  AVG(rp.avg_rating) DESC, SUM(rp.num_ratings) DESC
        ) AS category_rank
    FROM
        reviews_per_product rp
    INNER JOIN {{ ref('fact_product_category') }} AS pc
    ON rp.asin = pc.product_asin
    GROUP BY
        pc.category_id,
        rp.year_month
)
SELECT
    c.category_name AS category_name,
    t.avg_category_rating AS avg_category_rating,
    t.year_month AS year_month,
    CONCAT(
        'Top ',
        t.category_rank
    ) AS label
FROM
    ranked_categories as t
INNER JOIN {{ ref('dim_category') }} AS c
ON t.category_id = c.category_id
WHERE
    t.category_rank <= 5