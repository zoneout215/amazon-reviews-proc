import os
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.models.baseoperator import chain 
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from datetime import datetime, timedelta, timezone

GCP_PROJECT_ID = 'e-analogy-449921-p7'
BUCKET_NAME = 'bucket_amazon_reviews'
DATASET_NAME = 'amazon_reviews_dbt'
DIR_COMPRESSED = 'landing/snap.stanford.edu/data/amazon/productGraph/'
DIR_DECOMPRESSED = 'landing/decompressed/'

METADATA_RAW_TABLE = f'{GCP_PROJECT_ID}.{DATASET_NAME}.gcs_raw_metadata'
ITEMS_RAW_TABLE = f'{GCP_PROJECT_ID}.{DATASET_NAME}.gcs_raw_items'

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag_config = {
    'dag_id': '020_load_staging',
    'default_args': DEFAULT_ARGS,
    'schedule_interval': None,
    'max_active_runs': 1,
    'catchup': False,
    'start_date': datetime.now(timezone.utc)  
}

with DAG(**dag_config) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')   


    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_analytics_dataset",
        dataset_id=DATASET_NAME,
        project_id=GCP_PROJECT_ID,
        location='EU',
        if_exists="log", 
    )

    load_json_to_bigquery = GCSToBigQueryOperator(
            task_id = 'load_items_to_bigquery',
            bucket = BUCKET_NAME,
            source_objects = [os.path.join(DIR_DECOMPRESSED, "item_dedup.json")],
            destination_project_dataset_table = ITEMS_RAW_TABLE,
            autodetect=True,
            source_format='NEWLINE_DELIMITED_JSON', 
            write_disposition='WRITE_TRUNCATE',  # Overwrite table if exists
            create_disposition='CREATE_IF_NEEDED', 
        )
    
    load_metadata = BigQueryExecuteQueryOperator(
        task_id='load_metadata',
        sql=f"""
            CREATE OR REPLACE TABLE `{METADATA_RAW_TABLE}` (
                json_string STRING
            );

            LOAD DATA OVERWRITE `{METADATA_RAW_TABLE}` (json_string STRING)
            FROM FILES (
            format = 'CSV',
            field_delimiter = '\\u00FE', 
            quote = '',
            uris = ['gs://{BUCKET_NAME}/{DIR_DECOMPRESSED}metadata.json']);
        """,  
        use_legacy_sql=False, ## \\u00FE is a really rare delimeter which will read every JSON line into a separate record
    )

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_dbt_processing",
        trigger_dag_id="030_dbt_processing",
        wait_for_completion=False
    )

    chain(start, create_dataset, [load_json_to_bigquery, load_metadata],trigger_next, end)
