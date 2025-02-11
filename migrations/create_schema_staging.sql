CREATE OR REPLACE VIEW default.staging_metadata
AS
    SELECT
        asin,
        categories,
        description,
        title,
        price,
        salesRank as sales_rank,
        imUrl as im_url,
        related,
        created_at,
    FROM
        default.raw_data_metadata AS m;

CREATE OR REPLACE VIEW default.staging_items
AS 
    SELECT
        reviewerID,
        asin,
        reviewerName,
        helpful,
        reviewText,
        overall as rating,
        summary,
        unixReviewTime,
        reviewTime,
        created_at
    FROM
        default.raw_data_items AS r;

