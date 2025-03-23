from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import os
from airflow.models.baseoperator import chain


LOCAL_STORAGE_PATH = "/opt/airflow/data"
GCP_PROJECT_ID = "e-analogy-449921-p7"
BUCKET_NAME = "bucket_amazon_reviews"
DATASET_NAME = "amazon_reviews_dbt"
DIR_COMPRESSED = "landing/snap.stanford.edu/data/amazon/productGraph/"
DIR_DECOMPRESSED = "landing/decompressed/"

DEFAULT_ARGS = {
    "owner": "sergei.romanov",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag_config = {
    "dag_id": "040_flush_source_data",
    "default_args": DEFAULT_ARGS,
    "description": "An Airflow DAG to download and flush source data",
    "schedule_interval": "0 * * * *",
    "max_active_runs": 1,
    "catchup": False,
    "start_date": datetime(2025, 2, 8),
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


with DAG(**dag_config) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    download_gcs_data = PythonOperator(
        task_id="download_gcs_data",
        python_callable=download_from_gcs,
        op_kwargs={
            "bucket_name": BUCKET_NAME,
            "prefix": DIR_DECOMPRESSED,
            "local_path": "/path/to/local/storage/gcs_data",
        },
    )
    chain(start, download_gcs_data, end)
