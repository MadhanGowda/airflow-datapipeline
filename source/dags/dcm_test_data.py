"""Airflow Sample datapipeline DAG."""
import os
import sys
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, "/application/")

from source.dag_helper.sample_helper.dcm_helper import (  # noqa isort:skip
    read_distinct_raw_data_from_db,
    read_meta_data_from_db,
    split_placement_data,
    store_data_in_csv
)

default_args = {
    "owner": "airflow",
    "start_date": days_ago(100),
    "retries": os.environ.get("SPEND_LOAD_DAG_RETRIES", 0),
    "retry_delay": timedelta(minutes=1),
    "provide_context": False,
    "depends_on_past": False,
    "email": [],
    "email_on_retry": True,
    "email_on_failure": True
}


datapipeline_dag = DAG(
    "Database-DCM-DAG",
    description="",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)


start_task = DummyOperator(task_id="Start", dag=datapipeline_dag)
end_task = DummyOperator(task_id="End", dag=datapipeline_dag)


cleanup_data = PostgresOperator(
    task_id='cleanup_data',
    postgres_conn_id='airflow_postgres',
    dag=datapipeline_dag,
    sql=["TRUNCATE table test.DCM_TEST_DATA1;",
         "TRUNCATE table test.meta_data;"],
    autocommit=True,
    database='sample_airflow'
)


insert_raw_data = PostgresOperator(
    task_id='insert_raw_data',
    postgres_conn_id='airflow_postgres',
    dag=datapipeline_dag,
    sql='''COPY test.DCM_TEST_DATA1 FROM
    '/var/lib/postgresql/data/pgdata/DCM_TEST_DATA.csv'
    DELIMITER ',' CSV HEADER;''',
    autocommit=True,
    database='sample_airflow'
)


insert_meta_data = PostgresOperator(
    task_id='insert_meta_data',
    postgres_conn_id='airflow_postgres',
    dag=datapipeline_dag,
    sql='''COPY test.meta_data FROM
    '/var/lib/postgresql/data/pgdata/meta_data.csv'
    DELIMITER ',' CSV HEADER;''',
    autocommit=True,
    database='sample_airflow'
)


read_distinct_raw_data = PythonOperator(
    task_id='read_distinct_raw_data',
    dag=datapipeline_dag,
    provide_context=True,
    python_callable=read_distinct_raw_data_from_db,
)


read_meta_data = PythonOperator(
    task_id='read_meta_data',
    dag=datapipeline_dag,
    provide_context=True,
    python_callable=read_meta_data_from_db,
)


split_data = PythonOperator(
    task_id="split_data",
    dag=datapipeline_dag,
    provide_context=True,
    python_callable=split_placement_data,
)


store_data_csv = PythonOperator(
    task_id="store_data_csv",
    dag=datapipeline_dag,
    provide_context=True,
    python_callable=store_data_in_csv,
)


load_to_database = PostgresOperator(
    task_id='store_data_to_database',
    postgres_conn_id='airflow_postgres',
    dag=datapipeline_dag,
    sql='''COPY test.splitdata FROM
    '/var/lib/postgresql/data/pgdata/Split_Data.csv'
    DELIMITER ',' CSV HEADER;''',
    autocommit=True,
    database='sample_airflow'
)

start_task >> cleanup_data >> insert_raw_data
cleanup_data >> insert_meta_data
insert_raw_data >> read_distinct_raw_data
insert_meta_data >> read_meta_data
read_distinct_raw_data >> split_data
read_meta_data >> split_data
split_data >> store_data_csv
store_data_csv >> load_to_database
load_to_database >> end_task
