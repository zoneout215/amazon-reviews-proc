from datetime import datetime, timedelta, timezone
import os
from plugins.utils import parse_date, parse_time

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import (
    CloudDataTransferServiceCreateJobOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
DESTINATION_PATH_PREFIX = "landing/"
SOURCES_LINKS_DIR = "/opt/airflow/data_sources.tsv"

SCHEDULE = {
    "scheduleStartDate": parse_date(datetime.now(timezone.utc).date().strftime("%Y-%m-%d")),
    "scheduleEndDate": parse_date(datetime.now(timezone.utc).date().strftime("%Y-%m-%d")),
    "startTimeOfDay": parse_time(
        (datetime.now(tz=timezone.utc) + timedelta(minutes=1)).time().strftime("%H:%M:%S")
    ),
}

TRANSFER_BODY = {
    "description": "Transfer data from public URL to GCS",
    "status": "ENABLED",
    "projectId": GCP_PROJECT_ID,
    "schedule": SCHEDULE,
    "transferSpec": {
        "httpDataSource": {
            "listUrl": f"gs://{BUCKET_NAME}/landing/data_sources.tsv",
        },
        "gcsDataSink": {
            "bucketName": BUCKET_NAME,
            "path": DESTINATION_PATH_PREFIX,
        },
    },
}

DEFAULT_ARGS = {
    "owner": "sergei.romanov",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

dag_config = {
    "dag_id": "000_ingerstion_gcs",
    "default_args": DEFAULT_ARGS,
    "schedule_interval": None,
    "max_active_runs": 1,
    "catchup": False,
    "start_date": datetime.now(timezone.utc),
}


with DAG(**dag_config) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=BUCKET_NAME,
        storage_class="STANDARD",
        location="EU",
        exists_ok=True,
    )

    upload_task = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=SOURCES_LINKS_DIR,
        dst=DESTINATION_PATH_PREFIX,
        bucket=BUCKET_NAME,
    )

    create_transfer = CloudDataTransferServiceCreateJobOperator(
        task_id="create_transfer_job", project_id=GCP_PROJECT_ID, body=TRANSFER_BODY
    )

    sensor_task_metadata = GCSObjectExistenceSensor(
        task_id="sensor_task_metadata",
        bucket=BUCKET_NAME,
        object="landing/snap.stanford.edu/data/amazon/productGraph/metadata.json.gz",
        deferrable=True,
        poke_interval=300,
    )

    sensor_task_items = GCSObjectExistenceSensor(
        task_id="sensor_task_items",
        bucket=BUCKET_NAME,
        object="landing/snap.stanford.edu/data/amazon/productGraph/item_dedup.json.gz",
        deferrable=True,
        poke_interval=300,
    )

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_decompression",
        trigger_dag_id="010_decompression",
        wait_for_completion=False,
    )

    chain(
        start,
        create_bucket,
        upload_task,
        create_transfer,
        [sensor_task_metadata, sensor_task_items],
        trigger_next,
        end,
    )
