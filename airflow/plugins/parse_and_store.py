import datetime
import gzip
import json
import os
import pandas as pd
import re
from hdfs import InsecureClient
from typing import List, Tuple, Generator
from google.cloud import storage
import clickhouse_connect

HDFS_URI = os.getenv('HDFS_URI')
HDFS_CLIENT = InsecureClient(HDFS_URI, user='hdfs')

k = os.getenv('CLICKHOUSE_HOST')
CLICKHOUSE_PORT = 8123 #os.getenv('CLICKHOUSE_PORT')
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DB')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')
NUMBER_BATCHES = int(os.getenv('NUMBER_STAG_BATCHES'))
BATCH_SIZE = 1000000

HDFS_METADATA_PATH = '/data/items/metadata/metadata.json.gz'
HDFS_ITEMS_PATH = '/data/items/item_dedup/item_dedup.json.gz' 

def connect_to_clickhouse():
    """Creates and returns a ClickHouse client."""
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB
    )

def download_file_from_hdfs(hdfs_path: str, local_path: str):
    HDFS_CLIENT.download(hdfs_path, local_path, overwrite=True)
    print(f"Downloaded {hdfs_path} to {local_path} without decompression")
    

def read_and_parse_from_hdfs_in_batches(
    hdfs_path,
    batch_size=BATCH_SIZE,
    is_metadata=False
        ) -> Generator[List[Tuple[str]], None, None]:
    """
    Reads compressed JSON data from HDFS in batches. Uses list as buffer, and yields when buffer is full.
    """
    with HDFS_CLIENT.read(hdfs_path) as reader:
        with gzip.GzipFile(fileobj=reader) as content:
            buffer = []
            for line in content:
                record = parse_json_line(line, is_metadata)
                if record:
                    buffer.append(record)
                if len(buffer) >= batch_size:
                    yield buffer
                    buffer = [] # empyting buffer         
            if buffer:
                yield buffer



def read_and_parse_from_gcs_in_batches(
    gcs_bucket_name: str,
    gcs_blob_name: str,
    batch_size=BATCH_SIZE,
    is_metadata=False
) -> Generator[List[Tuple[str]], None, None]:
    """
    Reads compressed JSON data from Google Cloud Storage in batches.
    Uses list as buffer, and yields when buffer is full.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket_name)
    blob = bucket.blob(gcs_blob_name)

    with blob.download_to_filename('/tmp/data.gz') as tmp_file:
        with gzip.GzipFile(fileobj=tmp_file) as content:
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
        return record, datetime.datetime.now()
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
    """Processes metadata from HDFS and writes to ClickHouse in batches."""
    ch_client = connect_to_clickhouse()
    ch_client.command('TRUNCATE TABLE IF EXISTS default.raw_data_metadata')

    print("Processing metadata...")
    total_records = 0
    for batch in read_and_parse_from_hdfs_in_batches(HDFS_METADATA_PATH, is_metadata=True):
        ch_client.insert('default.raw_data_metadata', batch)
        total_records += len(batch)
        print(f"Ingested {total_records} metadata records")
    ch_client.close()


def process_items():
    """Processes items from HDFS and writes to ClickHouse in batches."""
    ch_client = connect_to_clickhouse()
    ch_client.command('TRUNCATE TABLE IF EXISTS default.raw_data_tems')
    batch_count = 0
    total_records = 0
    for batch in read_and_parse_from_hdfs_in_batches(HDFS_ITEMS_PATH):
        ch_client.insert('default.raw_data_items', batch)
        total_records += len(batch)
        print(f"Ingested {total_records} rating records")
        batch_count += 1
        if batch_count >= NUMBER_BATCHES:
            break
    ch_client.close()
