from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def print_hello():
    return "Hello, Airflow!"

default_args = {
    "start_date": days_ago(1),
}

simple_dag = DAG(
    'simple_dag',
    default_args=default_args,
    description='A simple DAG',
    schedule_interval=timedelta(days=1),
)

task_hello = PythonOperator(
    task_id='say_hello',
    python_callable=print_hello,
    dag=simple_dag,
)