"""CCLS reporting datapipeline file."""
import os
import sys
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, "/application/")

from source import utils  # noqa isort:skip

default_args = {
    "owner": "airflow",
    "start_date": days_ago(100),
    "retries": os.environ.get("SPEND_LOAD_DAG_RETRIES", 0),
    "retry_delay": timedelta(minutes=1),
    "provide_context": True,
    "depends_on_past": False,
    "email": utils.get_failure_email_list(),
    "email_on_retry": True,
    "email_on_failure": True
}

datapipeline_dag = DAG(
    "Backbone-Sample-DAG",
    description="",
    default_args=default_args,
    schedule_interval='0 8 * * *',
    catchup=False,
)

start_task = DummyOperator(task_id="Start", dag=datapipeline_dag)
end_task = DummyOperator(task_id="End", dag=datapipeline_dag)

start_task >> end_task
