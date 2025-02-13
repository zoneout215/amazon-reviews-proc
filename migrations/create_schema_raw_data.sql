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