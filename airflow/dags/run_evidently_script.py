run_evidently_script

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def run_evidently_script():
    import evidently_airflow_example1
    evidently_airflow_example1.main()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

with DAG(
    'evidently_example1',
    default_args=default_args,
    description='A simple DAG to run Evidently script',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    task_run_script = PythonOperator(
        task_id='run_evidently_script',
        python_callable=run_evidently_script,
    )
    task_run_script