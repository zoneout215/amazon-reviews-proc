from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

DEFAULT_ARGS = {
    "owner": "sergei.romanov",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

dag_config = {
    "dag_id": "030_dbt_processing",
    "default_args": DEFAULT_ARGS,
    "description": "An Airflow DAG to run DBT models",
    "schedule_interval": None,
    "max_active_runs": 1,
    "catchup": False,
    "start_date": datetime.now(timezone.utc),
}

with DAG(**dag_config) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    dbt_processing = BashOperator(
        task_id="dbt_processing",
        bash_command="cd /opt/airflow/dbt && dbt run --threads 100",
        retries=1,
        retry_delay=timedelta(minutes=1),
    )
    start >> dbt_processing >> end
