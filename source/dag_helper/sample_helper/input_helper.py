"""Airflow Sample datapipeline DAG."""
import pandas as pd


def read_data(*args, **kwargs):
    """
    Read CSV data.

    Input: CSV data from the CSV file.

    Returns: CSV data in dictionary format.
    """
    csv_data = pd.read_csv("source/data/input.csv")
    input_data = csv_data.to_dict('records')
    kwargs['ti'].xcom_push(
        key='unloaded_data',
        value=input_data
    )


def split_data(*args, **kwargs):
    """Splitting the CSV data.

    Input: CSV data read in the Previous task id.

    Returns: Split csv data as a dictionary object.
    """
    ti = kwargs['ti']
    csv_data = ti.xcom_pull(task_ids='sample_task', key='unloaded_data')
    csv_data1 = pd.DataFrame.from_records(csv_data)
    split_data_by_date = csv_data1.groupby(['Date']).head(50).to_dict(
        'records'
    )
    # split_data_by_date1 = json.dumps(split_data_by_date)
    ti.xcom_push(
        key='unloaded_data1',
        value=split_data_by_date
    )


def group_data(*args, **kwargs):
    """Split data individual files as per date.

    Input: Split CSV data from the previous task id.

    Returns: Individual files based on date.
    """
    ti = kwargs['ti']
    csv_data = ti.xcom_pull(task_ids='sample_task1', key='unloaded_data1')
    csv_data1 = pd.DataFrame.from_records(csv_data)
    split_data_by_date = csv_data1.groupby(['Date'])
    for group in split_data_by_date:
        print(group[0])
        print(group)
