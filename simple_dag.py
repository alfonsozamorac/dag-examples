from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def print_hello():
    return "Hello, Airflow!"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
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