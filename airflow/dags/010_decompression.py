from datetime import datetime, timedelta, timezone
from airflow import DAG
import os
from airflow.models.baseoperator import chain 
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

GCP_PROJECT_ID = 'e-analogy-449921-p7'
BUCKET_NAME = 'gs://bucket_amazon_reviews'
DIR_COMPRESSED = 'landing/snap.stanford.edu/data/amazon/productGraph/'
DIR_DECOMPRESSED = 'landing/decompressed/'
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': GCP_PROJECT_ID,
    'location': 'europe-west1',
}

dag_config = {
    'dag_id': '010_decompression',
    'default_args': DEFAULT_ARGS,
    'schedule_interval': None,
    'max_active_runs': 1,
    'catchup': False,
    'start_date': datetime.now(timezone.utc)  
}

with DAG(**dag_config) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')   
    
    decopress_metadata = DataflowTemplatedJobStartOperator(
        task_id='decopress_metadata',
        template='gs://dataflow-templates/latest/Bulk_Decompress_GCS_Files',
        parameters={
            'inputFilePattern': os.path.join(BUCKET_NAME, DIR_COMPRESSED, "metadata.json.gz"),
            'outputDirectory': os.path.join(BUCKET_NAME, DIR_DECOMPRESSED),
            'outputFailureFile': os.path.join(BUCKET_NAME, DIR_DECOMPRESSED, "failures_metadata.txt"),
        },
    )
    decopress_items = DataflowTemplatedJobStartOperator(
        task_id='decopress_items',
        template='gs://dataflow-templates/latest/Bulk_Decompress_GCS_Files',
        parameters={
            'inputFilePattern': os.path.join(BUCKET_NAME, DIR_COMPRESSED, "item_dedup.json.gz"),
            'outputDirectory': os.path.join(BUCKET_NAME, DIR_DECOMPRESSED),
            'outputFailureFile': os.path.join(BUCKET_NAME, DIR_DECOMPRESSED, "failures_items.txt"),
        },
    )

    sensor_task_metadata = GCSObjectExistenceSensor(
        task_id='sensor_task_metadata',
        bucket=BUCKET_NAME,
        object= os.path.join(DIR_DECOMPRESSED, "metadata.json.gz"),
    )

    sensor_task_items = GCSObjectExistenceSensor(
        task_id='sensor_task_items',
        bucket=BUCKET_NAME,
        object= os.path.join(DIR_DECOMPRESSED, "item_dedup.json.gz"),
    )
    chain(start, [decopress_metadata, decopress_items],[sensor_task_metadata, sensor_task_items], end)
