from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from bees_breweries_pipeline.tasks.landing_breweries_task import (
    LandingBreweriesTask,
)

from bees_breweries_pipeline.tasks.bronze_breweries_task import (
    BronzeBreweriesTask,
)

from bees_breweries_pipeline.tasks.silver_breweries_task import (
    SilverBreweriesTask,
)

from bees_breweries_pipeline.tasks.gold_breweries_task import GoldBreweriesTask

from bees_breweries_pipeline.tools.const.pipeline import Pipeline

default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 5,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id=Pipeline.DAG_NAME,
    description="Pipeline regarding breweries data",
    default_args=default_args,
    schedule_interval="0 4 * * *",  # Every day at 4am
    catchup=False,
    max_active_runs=1,
    tags=[f"source: {Pipeline.SOURCE}"],
) as dag:

    start = DummyOperator(task_id="start")

    extract_breweries = LandingBreweriesTask(
        task_id="extract_breweries_task",
        dag=dag,
    )

    transform_breweries = BronzeBreweriesTask(
        task_id="transform_breweries_task", dag=dag
    )

    treat_breweries = SilverBreweriesTask(
        task_id="treat_breweries_task", dag=dag
    )

    inject_breweries = GoldBreweriesTask(
        task_id="inject_breweries_task", dag=dag
    )

    end = DummyOperator(task_id="end")

    (
        start
        >> extract_breweries
        >> transform_breweries
        >> treat_breweries
        >> inject_breweries
        >> end
    )
