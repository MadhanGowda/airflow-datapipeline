"""Airflow Sample datapipeline DAG."""
import os
import sys
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, "/application/")

from source import utils  # noqa isort:skip
from source.dag_helper.sample_helper.sample_helper import sample_test_task  # noqa isort:skip
from source.dag_helper.sample_helper.sample_data import sample_variable  # noqa isort:skip

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
    "Airflow-Sample-DAG",
    description="",
    default_args=default_args,
    schedule_interval='0 8 * * *',
    catchup=False,
)

start_task = DummyOperator(task_id="Start", dag=datapipeline_dag)
end_task = DummyOperator(task_id="End", dag=datapipeline_dag)

sample_task = PythonOperator(
    task_id="sample_task",
    dag=datapipeline_dag,
    provide_context=True,
    python_callable=sample_test_task,
    op_kwargs={
        'sample_variable': sample_variable,
    },
)

start_task >> sample_task >> end_task
