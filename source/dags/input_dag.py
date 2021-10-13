"""Airflow Sample datapipeline DAG."""
import os
import sys
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, "/application/")

from source.dag_helper.sample_helper.input_helper import (
    read_data,
    split_data,
    group_data
)   # noqa isort:skip

default_args = {
    "owner": "airflow",
    "start_date": days_ago(100),
    "retries": os.environ.get("SPEND_LOAD_DAG_RETRIES", 0),
    "retry_delay": timedelta(minutes=1),
    "provide_context": True,
    "depends_on_past": False,
    "email": [],
    "email_on_retry": True,
    "email_on_failure": True
}

datapipeline_dag = DAG(
    "Data_Input_Separation_with_3_tasks",
    description="",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)


start_task = DummyOperator(task_id="Start", dag=datapipeline_dag)
end_task = DummyOperator(task_id="End", dag=datapipeline_dag)

read_raw_data = PythonOperator(
    task_id="read_raw_data",
    dag=datapipeline_dag,
    provide_context=True,
    python_callable=read_data,
)
split_data_by_date = PythonOperator(
    task_id="split_data_by_date",
    dag=datapipeline_dag,
    provide_context=True,
    python_callable=split_data,
)
group_data_by_date = PythonOperator(
    task_id="group_data_by_date",
    dag=datapipeline_dag,
    provide_context=True,
    python_callable=group_data,
)

start_task >> read_raw_data >> split_data_by_date
split_data_by_date >> group_data_by_date >> end_task
