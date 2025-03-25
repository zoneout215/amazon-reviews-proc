from datetime import datetime
import os
from airflow.providers.google.cloud.hooks.gcs import GCSHook


def parse_date(date: str):
    dt = datetime.strptime(date, "%Y-%m-%d")
    return {"year": dt.year, "month": dt.month, "day": dt.day}


def parse_time(time: str) -> dict:
    # Parse the time string into a datetime object
    dt = datetime.strptime(time, "%H:%M:%S")
    return {
        "hours": dt.hour,
        "minutes": dt.minute,
        "seconds": dt.second,
        "nanos": 0,  # Assuming nanoseconds are zero
    }


def download_from_gcs(bucket_name, prefix, local_path):
    """Download data from Google Cloud Storage"""
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")

    if not os.path.exists(local_path):
        os.makedirs(local_path)

    blobs = gcs_hook.list_blobs(bucket_name=bucket_name, prefix=prefix)
    for blob in blobs:
        local_file_path = os.path.join(local_path, os.path.basename(blob.name))
        gcs_hook.download(bucket_name=bucket_name, object_name=blob.name, filename=local_file_path)
