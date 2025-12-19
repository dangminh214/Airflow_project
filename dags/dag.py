import logging
from airflow import DAG # type: ignore
from datetime import datetime
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator # type: ignore
from etl_scripts.fetch_api import fetch_api_data
from etl_scripts.scraper import scrape_yahoo_finance_news
from airflow.operators.bash import BashOperator # type: ignore


default_args = {
    "owner": "minh",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="minh_dag",
    start_date=datetime(2025, 12, 19), 
    schedule="@daily",
    tags=["scrapping", "finance", "postgres"]
) as dag:
    scrape_and_load = PythonOperator(
        task_id="scrape_finance_data",
        python_callable=scrape_yahoo_finance_news,
    )

    scrape_and_load 