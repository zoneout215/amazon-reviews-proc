CREATE OR REPLACE VIEW default.staging_metadata
AS
    SELECT
        JSONExtractString(m.json_string, 'asin') AS asin,
        arrayJoin(
            arrayJoin(
                JSONExtract(
                    json_string, 'categories', 'Array(Array(String))')
                )) as categories,
        JSONExtractString(m.json_string, 'description') AS description,
        JSONExtractString(m.json_string, 'title') AS title,
        JSONExtractFloat(m.json_string, 'price') AS price,
        JSONExtractInt(m.json_string, 'salesRank') AS salesRank,
        JSONExtractString(m.json_string, 'imUrl') AS imUrl,
        JSONExtractString(m.json_string, 'related') AS related,
        created_at,
    FROM
        default.raw_data_metadata AS m;

CREATE OR REPLACE VIEW default.staging_items
AS 
    SELECT
        JSONExtractString(r.json_string, 'reviewerID') AS reviewerID,
        JSONExtractString(r.json_string, 'asin') AS asin,
        JSONExtractString(r.json_string, 'reviewerName') AS reviewerName,
        JSONExtractInt(r.json_string, 'helpful') AS helpful,
        JSONExtractString(r.json_string, 'reviewText') AS reviewText,
        JSONExtractFloat(r.json_string, 'overall') AS rating,
        JSONExtractString(r.json_string, 'summary') AS summary,
        JSONExtractInt(r.json_string, 'unixReviewTime') AS unixReviewTime,
        JSONExtractString(r.json_string, 'reviewTime') AS reviewTime,
        created_at
    FROM
        default.raw_data_items AS r;