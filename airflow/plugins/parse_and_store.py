import datetime
import gzip
import json
import os
import re
from google.cloud import storage, bigquery
import gzip
from typing import Generator, List, Tuple


HDFS_URI = os.getenv('HDFS_URI')

CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST')
CLICKHOUSE_PORT = 8123 
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DB')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')

NUMBER_BATCHES = int(os.getenv('NUMBER_STAG_BATCHES'))
BATCH_SIZE = 10000
ITEMS_TABLE_ID = 'e-analogy-449921-p7.amazon_reviews.raw_data_items'
METADATA_TABLE_ID = 'e-analogy-449921-p7.amazon_reviews.raw_data_metadata'
METADATA_PATH = "landing/snap.stanford.edu/data/amazon/productGraph/metadata.json.gz"
ITEMS_PATH = "landing/snap.stanford.edu/data/amazon/productGraph/item_dedup.json.gz"
BUCKET_NAME = 'jet-assignment-12312'


def connect_to_bq():
    """Creates and returns a BigQuery client."""
    return bigquery.Client()


def read_and_parse_from_gcs_in_batches(
    bucket_name: str,
    blob_name: str,
    batch_size: int = BATCH_SIZE,
    is_metadata=False
) -> Generator[List[Tuple[str]], None, None]:
    """Reads compressed JSON data from Google Cloud Storage in batches using streaming."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
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


def parse_json_line(line: bytes, is_metadata=False) -> dict[str, str]:
    """Parses a line of text containing JSON."""
    try:
        line_str = line.decode('utf-8')
        if is_metadata:
            line_str = clean_json_line(line_str)
        record = json.dumps(json.loads(line_str))
        return {"json_string": record, "created_at": datetime.datetime.now().strftime("%Y-%m-%d")}
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


def process_metadata():
    """Processes metadata from GSC and writes to BigQuery in batches."""
    bq_client = connect_to_bq()
    print("Processing metadata...")
    total_records = 0
    for batch in read_and_parse_from_gcs_in_batches(BUCKET_NAME, METADATA_PATH, is_metadata=True):
        errors = bq_client.insert_rows_json(METADATA_TABLE_ID, batch) # store errors in a list
        if errors == []:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))
        total_records += (len(batch) - len(errors))
        errors = [] # emptying errors list
        print(f"Ingested {total_records} metadata records")
        break


def process_items():
    """Processes items from GSC and writes to BigQuery in batches."""
    bq_client = connect_to_bq()
    print("Processing metadata...")
    total_records = 0
    for batch in read_and_parse_from_gcs_in_batches(BUCKET_NAME, ITEMS_PATH, is_metadata=False):
        errors = bq_client.insert_rows_json(ITEMS_TABLE_ID, batch) # store errors in a list
        if errors == []:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))
        total_records += (len(batch) - len(errors))
        errors = [] # emptying errors list
        print(f"Ingested {total_records} metadata records")
        break
