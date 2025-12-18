import logging
from airflow import DAG # type: ignore
from datetime import datetime
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator # type: ignore
from etl_scripts.fetch_api import fetch_api_data
from airflow.operators.bash import BashOperator # type: ignore


default_args = {
    "owner": "minh",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="minh_dag",
    start_date=datetime(2025, 12, 16),
    schedule="@daily",
) as dag:
    fetch_data = PythonOperator(
        task_id="fetch_api_data",
        python_callable=fetch_api_data,
    )

    fetch_data