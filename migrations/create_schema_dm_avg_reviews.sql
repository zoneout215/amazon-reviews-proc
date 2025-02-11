CREATE TABLE IF NOT EXISTS default.dm_rating_top_bottom_five (
    label      VARCHAR(128),
    movie      VARCHAR(256),
    year_month VARCHAR(8)
)
ENGINE MergeTree() 
ORDER BY year_month;

CREATE TABLE IF NOT EXISTS default.dm_rating_top_five_increased_rating (
    label      VARCHAR(128),
    movie      VARCHAR(256),
    year_month VARCHAR(8)
)
ENGINE MergeTree() 
ORDER BY year_month;
