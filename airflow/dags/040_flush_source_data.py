from datetime import datetime, timedelta

from plugins.flush_source_data import flush_items, flush_metadata

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

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
    "description": "An Airflow DAG to flush files to local storage",
    "schedule_interval": "0 * * * *",
    "max_active_runs": 1,
    "catchup": False,
    "start_date": datetime(2025, 2, 8),
}

with DAG(**dag_config) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")
    flush_metadata = PythonOperator(
        task_id="flush_metadata",
        python_callable=flush_metadata,
    )

    flush_items = PythonOperator(
        task_id="flush_items",
        python_callable=flush_items,
    )

    start >> [flush_items, flush_metadata] >> end
