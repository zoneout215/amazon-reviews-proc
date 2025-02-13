from datetime import datetime, timedelta
import os
from typing import List

from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

SCHEMAS_DIR = "/opt/airflow/migrations/"

DEFAULT_ARGS = {
    'owner': 'sergei.romanov',
    'clickhouse_conn_id': 'clickhouse',
    'database': 'default',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

dag_config = {
    'dag_id': '020_run_migration',
    'default_args': DEFAULT_ARGS,
    'schedule_interval': None,
    'max_active_runs': 1,
    'catchup': False,
    'start_date': datetime(2025, 2, 8)
}

def read_sql_file(folder_path: str, filename: str) -> List[str]:
    """Reads a SQL file and returns a list of queries from the file."""
    file_path = os.path.join(folder_path, filename)
    if os.path.exists(file_path) and filename.endswith('.sql'):
        with open(file_path, 'r') as file:
            content = file.read()
            queries = [query.strip() for query in content.split(';') if query.strip()]
            return queries

with DAG(**dag_config) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    create_schema_raw_data = ClickHouseOperator(
        task_id='create_schema_raw_data',
        sql=read_sql_file(SCHEMAS_DIR, "create_schema_raw_data.sql"),
    )
 
    create_schema_staging = ClickHouseOperator(
        task_id='create_schema_staging',
        sql=read_sql_file(SCHEMAS_DIR, "create_schema_staging.sql"),
    )

    create_schema_core_dwh = ClickHouseOperator(
        task_id='create_schema_core_dwh',
        sql=read_sql_file(SCHEMAS_DIR, "create_schema_core_dwh.sql"),
    )
    
    create_schema_dm_avg_reviews = ClickHouseOperator(
        task_id='create_schema_dm_avg_reviews',
        sql=read_sql_file(SCHEMAS_DIR, "create_schema_dm_avg_reviews.sql"),
    )

    trigger_hdfs_to_pg = TriggerDagRunOperator(
        task_id="trigger_hdfs_to_pg",
        trigger_dag_id="021_load_staging",
        wait_for_completion=False
    )

    schema_creation = [create_schema_staging, create_schema_core_dwh, create_schema_dm_avg_reviews]
    start >> create_schema_raw_data >> schema_creation >> trigger_hdfs_to_pg >> end
