from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def print_hello():
    return "Hello, Airflow!"

def print_goodbye():
    return "Goodbye, Airflow!"

def print_middle():
    return "Executing intermediate task"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

complex_dag = DAG(
    'complex_dag',
    default_args=default_args,
    description='A DAG with dependencies',
    schedule_interval=timedelta(days=1),
)

task_start = PythonOperator(
    task_id='start',
    python_callable=print_hello,
    dag=complex_dag,
)

task_middle = PythonOperator(
    task_id='middle',
    python_callable=print_middle,
    dag=complex_dag,
)

task_end = PythonOperator(
    task_id='end',
    python_callable=print_goodbye,
    dag=complex_dag,
)

task_start >> task_middle >> task_end
