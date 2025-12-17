from airflow import DAG # type: ignore
from datetime import datetime

dag = DAG(
    dag_id="sample_dag",
    description="This the first dag to practice",
    start_date=datetime(2025, 12, 11),
    schedule_interval="@daily",
    catchup=False,
)