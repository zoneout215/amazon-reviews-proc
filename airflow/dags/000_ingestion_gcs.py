from airflow import DAG
from datetime import datetime, timedelta, timezone, date
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import (
    CloudDataTransferServiceCreateJobOperator
)
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models.baseoperator import chain 
from plugins.ingest import parse_date, parse_time

DEFAULT_ARGS = {
    'owner': 'sergei.romanov',
    'clickhouse_conn_id': 'clickhouse',
    'database': 'default',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

dag_config = {
    'dag_id': '000_ingerstion_gcs',
    'default_args': DEFAULT_ARGS,
    'schedule_interval': None,
    'max_active_runs': 1,
    'catchup': False,
    'start_date': datetime.now(timezone.utc)  
}

GCP_PROJECT_ID = 'e-analogy-449921-p7'
URL_LIST = "gs://jet-assignment-211312/test/test.tsv" 
METADATA_LINK ="https://snap.stanford.edu/data/amazon/productGraph/metadata.json.gz"
RATINGS_LINK =  "https://snap.stanford.edu/data/amazon/productGraph/item_dedup.json.gz"
DESTINATION_BUCKET = 'jet-assignment-12312'
DESTINATION_PATH_PREFIX = 'landing/'  # Optional path prefix in the bucket

SCHEDULE = {
        "scheduleStartDate":  parse_date(datetime.now(timezone.utc).date().strftime("%Y-%m-%d")),
        "scheduleEndDate": parse_date(datetime.now(timezone.utc).date().strftime("%Y-%m-%d")),
        "startTimeOfDay": parse_time((datetime.now(tz=timezone.utc) + timedelta(minutes=1)).time().strftime("%H:%M:%S")),
    }

with DAG(**dag_config) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    # Create the transfer job    
    create_transfer = CloudDataTransferServiceCreateJobOperator(
        task_id='create_transfer_job',
        project_id=GCP_PROJECT_ID,
        body={
            'description': 'Transfer data from public URL to GCS',
            'status': 'ENABLED',
            'projectId': GCP_PROJECT_ID,
            'schedule': SCHEDULE,
            'transferSpec': {
                'httpDataSource': {
                    'listUrl': URL_LIST,
                },
                'gcsDataSink': {
                    'bucketName': DESTINATION_BUCKET,
                    'path': DESTINATION_PATH_PREFIX,
                }
            },
        }
    )
    chain(start, create_transfer, end)
