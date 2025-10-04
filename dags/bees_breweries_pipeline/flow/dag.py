from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from bees_breweries_pipeline.tasks.landing_breweries_task import (
    LandingBreweriesTask,
)

from tools.const.pipeline import Pipeline

default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 5,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id=Pipeline.DAG_NAME,
    description="Pipeline regarding breweries data",
    default_args=default_args,
    schedule_interval="0 4 * * *",  # todo dia às 4h
    catchup=False,
    max_active_runs=1,
    tags=[f"source: {Pipeline.SOURCE}"],
) as dag:

    start = DummyOperator(task_id="start")

    extract_breweries = PythonOperator(
        task_id="landing_breweries_task",
        python_callable=LandingBreweriesTask.call,
        dag=dag,
    )

    end = DummyOperator(task_id="end")

    start >> extract_breweries >> end
