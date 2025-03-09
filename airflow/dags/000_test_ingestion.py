from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import (
    CloudDataTransferServiceCreateJobOperator
)
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import chain 


DEFAULT_ARGS = {
    'owner': 'sergei.romanov',
    'clickhouse_conn_id': 'clickhouse',
    'database': 'default',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

dag_config = {
    'dag_id': '000_test_gse',
    'default_args': DEFAULT_ARGS,
    'schedule_interval': None,
    'max_active_runs': 1,
    'catchup': False,
    'start_date': datetime(2025, 2, 8)
}

# Define your GCP project and bucket details
GCP_PROJECT_ID = 'e-analogy-449921-p7'
METADATA_LINK ="https://snap.stanford.edu/data/amazon/productGraph/metadata.json.gz"
RATINGS_LINK =  "https://snap.stanford.edu/data/amazon/productGraph/item_dedup.json.gz"
SOURCE_URL = 'https://example.com/path/to/file.csv'  # Your public URL
DESTINATION_BUCKET = 'jet-assignment-12312'
DESTINATION_PATH_PREFIX = 'landing'  # Optional path prefix in the bucket

with DAG(**dag_config) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    # Create the transfer job

    # transfer_task = BashOperator(
    #     task_id='transfer_to_gcs',
    #     bash_command='gcloud transfer jobs create {public_url} gs://{bucket_name}/{object_name}',
    #     env={
    #         'public_url': METADATA_LINK,
    #         'bucket_name': DESTINATION_BUCKET,
    #         'object_name': 'data/metadata.json.gz',
    #     },
    # )

    
    create_transfer = CloudDataTransferServiceCreateJobOperator(
        task_id='create_transfer_job',
        project_id=GCP_PROJECT_ID,
        body={
            'description': 'Transfer data from public URL to GCS',
            'status': 'ENABLED',
            'projectId': GCP_PROJECT_ID,
            'transferSpec': {
                'httpDataSource': {
                    'listUrl': METADATA_LINK,
                },
                'gcsDataSink': {
                    'bucketName': DESTINATION_BUCKET,
                    'path': DESTINATION_PATH_PREFIX,
                }
            },
        }
    )
    chain(start, create_transfer, end)
