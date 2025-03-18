import os
import gzip
import requests
from os.path import join
from datetime import datetime

FILES_DIR_PATH = "/tmp/downloads/data"
METADATA_LINK ="https://snap.stanford.edu/data/amazon/productGraph/metadata.json.gz"
RATINGS_LINK =  "https://snap.stanford.edu/data/amazon/productGraph/item_dedup.json.gz"
HDFS_URI = os.getenv('HDFS_URI')


# def download_file(url: str, file_path: str, chunk_size: int = 524288): # in 512 MB
#     """Downloads a file from a given URL in chunks and saves it to a local path, up to a maximum size."""
#     print(f"Downloading file from {url}")
#     try:
#         target_size = get_http_file_size(url)
#         response = requests.get(url, stream=True, allow_redirects=True)
#         response.raise_for_status()

#         downloaded_size = 0
        
#         with open(file_path, 'wb') as f:
#             for chunk in response.iter_content(chunk_size=chunk_size):
#                 if chunk:  # Filter out keep-alive new chunks
#                     f.write(chunk)
#                     downloaded_size += len(chunk) / (1024 * 1024)
#                     print(f"Downloaded {downloaded_size}/{target_size} MB ")
#         print("Download complete!")

#     except requests.RequestException as e:
#         print(f"Error downloading {url}: {e}")
#         raise


# def get_http_file_size(url: str) -> int:
#     """Returns the size of a file in MB given its URL."""
#     response = requests.head(url)
#     size_in_bytes = int(response.headers.get("Content-Length", 0))
#     return size_in_bytes / (1024 * 1024)


# def ensure_hdfs_directory_exists(directory_path: str):
#     """Check if a directory exists in HDFS at the specified path, and if not, create it."""
#     try:
#         if not hdfs_client.content(directory_path, strict=False):
#             hdfs_client.makedirs(directory_path)
#             print(f"Directory {directory_path} created in HDFS.")
#         else:
#             print(f"Directory {directory_path} already exists in HDFS.")
#     except Exception as e:
#         print(f"An error occurred while accessing HDFS: {e}")


# def store_in_hdfs(local_path: str, hdfs_path: str):
#     """Stores a file from a local path to an HDFS path."""
#     try:
#         print(f"Uploading file to {hdfs_path}")
#         ensure_hdfs_directory_exists(hdfs_path)
#         hdfs_client.upload(
#             hdfs_path, local_path, overwrite=True, chunk_size=262144, n_threads = - 1) # in 512 MB

#     except Exception as e:
#         print(f"Error storing file in HDFS {hdfs_path}: {e}")
#         raise


# def data_consumption():
#     """Downloads metadata and ratings files from specified URLs and stores them locally."""
#     metadata_file_path = join(FILES_DIR_PATH, 'metadata.json.gz')
#     ratings_file_path = join(FILES_DIR_PATH, 'item_dedup.json.gz')

#     download_file(METADATA_LINK, metadata_file_path, chunk_size = 524288) 
#     download_file(RATINGS_LINK, ratings_file_path, chunk_size = 524288)


# def data_storage():
#     """Uploads locally stored metadata and ratings files to HDFS."""
#     metadata_file_path = join(FILES_DIR_PATH, 'metadata.json.gz')
#     ratings_file_path = join(FILES_DIR_PATH, 'item_dedup.json.gz')

#     store_in_hdfs(metadata_file_path, '/data/items/metadata')
#     store_in_hdfs(ratings_file_path, '/data/items/item_dedup')


def parse_date(date: str):
    dt = datetime.strptime(date, '%Y-%m-%d')
    return {
        "year": dt.year,
        "month": dt.month,
        "day": dt.day
    }

def parse_time(time: str) -> dict:
    # Parse the time string into a datetime object
    dt = datetime.strptime(time, '%H:%M:%S')
    return {
        "hours": dt.hour,
        "minutes": dt.minute,
        "seconds": dt.second,
        "nanos": 0  # Assuming nanoseconds are zero
    }