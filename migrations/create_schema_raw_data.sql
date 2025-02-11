-- CREATE TABLE IF NOT EXISTS default.raw_data_metadata (
--     asin            VARCHAR,
--     categories      VARCHAR,
--     description     TEXT,
--     title           VARCHAR,
--     brand           VARCHAR,
--     price           VARCHAR,
--     salesRank       VARCHAR,
--     imUrl           VARCHAR,
--     related         VARCHAR,
--     created_at      DATE
-- )
-- ENGINE MergeTree() 
-- ORDER BY created_at;

-- CREATE TABLE IF NOT EXISTS default.raw_data_items (
--     reviewerID       VARCHAR,
--     asin             VARCHAR,
--     reviewerName     VARCHAR,
--     helpful          VARCHAR,
--     reviewText       TEXT,
--     overall          VARCHAR,
--     summary          VARCHAR,
--     unixReviewTime   VARCHAR,
--     reviewTime       VARCHAR,
--     created_at          DATE
-- )
-- ENGINE MergeTree()
-- ORDER BY created_at;
-- ORDER BY reviewTime;


-- CREATE TABLE IF NOT EXISTS default.raw_data_items (
--     reviewerID       VARCHAR,
--     asin             VARCHAR,
--     reviewerName     VARCHAR,
--     helpful          VARCHAR,
--     reviewText       TEXT,
--     overall          VARCHAR,
--     summary          VARCHAR,
--     unixReviewTime   VARCHAR,
--     reviewTime       VARCHAR,
--     created_at          DATE
-- )
-- ENGINE MergeTree()
-- ORDER BY created_at;
-- ORDER BY reviewTime;


CREATE TABLE IF NOT EXISTS default.raw_data_metadata (
    json_string     TEXT,
    created_at      DATE
)
ENGINE MergeTree() 
ORDER BY created_at;


CREATE TABLE IF NOT EXISTS default.raw_data_items (
    json_string     TEXT,
    created_at      DATE
)
ENGINE MergeTree() 
ORDER BY created_at;


-- CREATE TABLE IF NOT EXISTS default.tmp_categories (
--     asin            VARCHAR,
--     categories      Array(String),
-- )
-- ENGINE MergeTree() 
-- ORDER BY asin;