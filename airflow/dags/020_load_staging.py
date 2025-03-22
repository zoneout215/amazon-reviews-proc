import os
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.models.baseoperator import chain 
from datetime import datetime, timedelta, timezone, date

GCP_PROJECT_ID = 'e-analogy-449921-p7'
BUCKET_NAME = 'bucket_amazon_reviews'
DIR_COMPRESSED = 'landing/snap.stanford.edu/data/amazon/productGraph/'
DIR_DECOMPRESSED = 'landing/decompressed/'

METADATA_RAW_TABLE = 'e-analogy-449921-p7.amazon_reviews.raw_metadata_gcs'
ITEMS_RAW_TABLE = 'e-analogy-449921-p7.amazon_reviews.raw_items_gcs'

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

    load_json_to_bigquery = GCSToBigQueryOperator(
            task_id = 'load_items_to_bigquery',
            bucket = BUCKET_NAME,
            source_objects = [os.path.join(DIR_DECOMPRESSED, "metadata.json")],
            destination_project_dataset_table = ITEMS_RAW_TABLE,
            autodetect=True,
            source_format='NEWLINE_DELIMITED_JSON', 
            write_disposition='WRITE_TRUNCATE',  # Overwrite table if exists
            create_disposition='CREATE_IF_NEEDED', 
        )
    
    load_metadata = BigQueryExecuteQueryOperator(
        task_id='load_metadata',
        sql="""
            TRUNCATE TABLE `e-analogy-449921-p7.amazon_reviews.raw_metadata_gcs`;

            LOAD DATA OVERWRITE `e-analogy-449921-p7.amazon_reviews.raw_metadata_gcs` (json_string STRING)
            FROM FILES (
            format = 'CSV',
            field_delimiter = '\\u00FE', 
            quote = '',
            uris = ['gs://jet-assignment-12312/decompressed/metadata.json']);
        """,  
        use_legacy_sql=False, ## \\u00FE is a really rare delimeter which will read every JSON line into a separate record
    )

    chain(start, [load_json_to_bigquery, load_metadata], end)