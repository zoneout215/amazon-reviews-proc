import os

HDFS_URI = os.getenv("HDFS_URI")
# HDFS_CLIENT = InsecureClient(HDFS_URI, user='hdfs')
HDFS_METADATA_PATH = "/data/items/metadata/metadata.json.gz"
HDFS_ITEMS_PATH = "/data/items/item_dedup/item_dedup.json.gz"
FILES_DIR_PATH = "/opt/airflow/source_data"


# def download_file_from_hdfs(hdfs_path: str, local_path: str):
#     HDFS_CLIENT.download(hdfs_path, local_path, overwrite=True)
#     print(f"Downloaded {hdfs_path} to {local_path} without decompression")


# def flush_metadata():
#     metadata_file_path = os.join(FILES_DIR_PATH, 'metadata.json.gz')
#     download_file_from_hdfs(HDFS_METADATA_PATH, metadata_file_path)
#     print("Metadata flushed to local storage")

# def flush_items():
#     ratings_file_path = os.join(FILES_DIR_PATH, 'item_dedup.json.gz')
#     download_file_from_hdfs(HDFS_ITEMS_PATH, ratings_file_path)
#     print("Items flushed to local storage")
