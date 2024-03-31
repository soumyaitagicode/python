from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

def process_file(filename, effective_date, **kwargs):
    print(f"Processing file: {filename} with effective date: {effective_date}")
    # Your file processing logic goes here

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

with DAG('process_files_dag', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    # Define the task to process the file
    task_process_file = PythonOperator(
        task_id=f'process_file_{{{{ execution_date.strftime("%Y%m%d") }}}}_{filename}',
        python_callable=process_file,
        op_kwargs={'filename': '{{ dag_run.conf["filename"] }}', 'effective_date': '{{ dag_run.conf["effective_date"] }}'},
        provide_context=True
    )
