from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'example_dag',
    default_args=default_args,
    description='An example DAG demonstrating Python and Bash operators',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['example'],
)

def print_hello():
    print("Sarah says hello from Airflow in Docker!")
    return 'Hello, Airflow!'

task1 = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

task2 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

task3 = BashOperator(
    task_id='sleep_task',
    bash_command='sleep 5',
    dag=dag,
)

task1 >> task2 >> task3