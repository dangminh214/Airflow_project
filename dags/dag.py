from airflow import DAG # type: ignore
from datetime import datetime
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime, timedelta
from scripts import fetch_api_data


default_args = {
    "owner": "minh",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fetch_api_daily",
    default_args=default_args,
    description="Fetch data from external API daily",
    schedule_interval="@daily",
    start_date=datetime(2025, 12, 19),
    catchup=False,
    tags=["api", "etl"],
) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_api_data",
        python_callable=fetch_api_data,
        provide_context=True,
    )

    fetch_data