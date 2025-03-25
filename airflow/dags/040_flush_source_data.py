from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from plugins.utils import download_from_gcs
import os

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
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

with DAG(**dag_config) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    download_gcs_data = PythonOperator(
        task_id="download_gcs_data",
        python_callable=download_from_gcs,
        op_kwargs={
            "bucket_name": BUCKET_NAME,
            "prefix": DIR_DECOMPRESSED,
            "local_path": "/opt/airflow/data",
        },
    )
    chain(start, download_gcs_data, end)
