import datetime
import gzip
import json
import os
import pandas as pd
import re
from google.cloud import storage, bigquery
import gzip
import io
from typing import Generator, List, Tuple
from typing import List, Tuple, Generator


NUMBER_BATCHES = int(os.getenv('NUMBER_STAG_BATCHES'))
BATCH_SIZE = 10000
ITEMS_TABLE_ID = 'e-analogy-449921-p7.amazon_reviews.raw_data_items'
METADATA_TABLE_ID = 'e-analogy-449921-p7.amazon_reviews.raw_data_metadata'
HDFS_METADATA_PATH = '/data/items/metadata/metadata.json.gz'
HDFS_ITEMS_PATH = '/data/items/item_dedup/item_dedup.json.gz' 
METADATA_PATH = "landing/snap.stanford.edu/data/amazon/productGraph/metadata.json.gz"
BUCKET_NAME = 'jet-assignment-12312'
# def connect_to_clickhouse():
#     """Creates and returns a ClickHouse client."""
#     return clickhouse_connect.get_client(
#         host=CLICKHOUSE_HOST,
#         port=CLICKHOUSE_PORT,
#         username=CLICKHOUSE_USER,
#         password=CLICKHOUSE_PASSWORD,
#         database=CLICKHOUSE_DB
#     )

def connect_to_bq():
    """Creates and returns a BigQuery client."""
    return bigquery.Client()

# def read_and_parse_from_hdfs_in_batches(
#     hdfs_path,
#     batch_size=BATCH_SIZE,
#     is_metadata=False
#         ) -> Generator[List[Tuple[str]], None, None]:
#     """
#     Reads compressed JSON data from HDFS in batches. Uses list as buffer, and yields when buffer is full.
#     """
#     with HDFS_CLIENT.read(hdfs_path) as reader:
#         with gzip.GzipFile(fileobj=reader) as content:
#             buffer = []
#             for line in content:
#                 record = parse_json_line(line, is_metadata)
#                 if record:
#                     buffer.append(record)
#                 if len(buffer) >= batch_size:
#                     yield buffer
#                     buffer = [] # empyting buffer         
#             if buffer:
#                 yield buffer


def read_and_parse_from_gcs_in_batches(
    bucket_name,
    blob_name,
    batch_size=BATCH_SIZE,
    is_metadata=False
) -> Generator[List[Tuple[str]], None, None]:
    """
    Reads compressed JSON data from Google Cloud Storage in batches using streaming.
    Uses same approach as HDFS streaming, with a fileobj reader.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    # Use the blob's media_io_stream to get a streaming reader
    with blob.open("rb") as reader:
        with gzip.GzipFile(fileobj=reader) as content:
            buffer = []
            for line in content:
                record = parse_json_line(line, is_metadata)
                if record:
                    buffer.append(record)
                if len(buffer) >= batch_size:
                    yield buffer
                    buffer = []  # emptying buffer         
            if buffer:
                yield buffer


def parse_json_line(line: bytes, is_metadata=False) -> Tuple[str, datetime.datetime]:
    """Parses a line of text containing JSON."""
    try:
        line_str = line.decode('utf-8')
        if is_metadata:
            line_str = clean_json_line(line_str)
        record = json.dumps(json.loads(line_str))
        return {"json_string": record, "created_at": datetime.datetime.now().date}
    except json.JSONDecodeError as e:
        return None     
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None
    
    
def clean_json_line(line):
    """
    Cleans a line of text containing JSON with single quotes.
    Finds singe quotes inside double quotes and drops them, then replaces singe quotes with double  quotes
    """
    line = re.sub(r'"([^"]*)"', lambda match: match.group(0).replace("'", ""), line)
    return line.replace("'", '"').replace('\\\"', "\'")


# def process_metadata():
#     """Processes metadata from HDFS and writes to ClickHouse in batches."""
#     ch_client = connect_to_clickhouse()
#     ch_client.command('TRUNCATE TABLE IF EXISTS default.raw_data_metadata')

#     print("Processing metadata...")
#     total_records = 0
#     for batch in read_and_parse_from_gcs_in_batches(HDFS_METADATA_PATH, is_metadata=True):
#         ch_client.insert('default.raw_data_metadata', batch)
#         total_records += len(batch)
#         print(f"Ingested {total_records} metadata records")
#     ch_client.close()

def process_metadata():
    """Processes metadata from HDFS and writes to ClickHouse in batches."""
    bq_client = connect_to_bq()
    # ch_client.command('TRUNCATE TABLE IF EXISTS default.raw_data_metadata')
    

    print("Processing metadata...")
    total_records = 0
    for batch in read_and_parse_from_gcs_in_batches(BUCKET_NAME, METADATA_PATH, is_metadata=True):
        bq_client.insert_rows_json(METADATA_TABLE_ID, batch)
        print(batch)
        total_records += len(batch)
        print(f"Ingested {total_records} metadata records")
        break

# def process_items():
#     """Processes items from HDFS and writes to ClickHouse in batches."""
#     ch_client = connect_to_clickhouse()
#     ch_client.command('TRUNCATE TABLE IF EXISTS default.raw_data_tems')
#     batch_count = 0
#     total_records = 0
#     for batch in read_and_parse_from_gcs_in_batches(HDFS_ITEMS_PATH):
#         ch_client.insert('default.raw_data_items', batch)
#         total_records += len(batch)
#         print(f"Ingested {total_records} rating records")
#         batch_count += 1
#         if batch_count >= NUMBER_BATCHES:
#             break
#     ch_client.close()
