import re
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

def process_file(filename, effective_date, **kwargs):
    print(f"Processing file: {filename} with effective date: {effective_date}")
    # Your file processing logic goes here

def validate_filename(filename):
    # Check if the filename is alphanumeric
    if not filename.isalnum():
        raise ValueError("Filename must be alphanumeric.")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

# Define the filename and effective_date dynamically using Jinja templating
filename = "{{ dag_run.conf['filename'] }}"
effective_date = "{{ dag_run.conf['effective_date'] }}"

with DAG('process_files_dag', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    # Validate filename
    task_validate_filename = PythonOperator(
        task_id='validate_filename',
        python_callable=validate_filename,
        op_args=[filename]
    )

    # Define the task to process the file
    task_process_file = PythonOperator(
        task_id=f'process_file_{{{{ execution_date.strftime("%Y%m%d") }}}}_{filename}',
        python_callable=process_file,
        op_kwargs={'filename': filename, 'effective_date': effective_date},
        provide_context=True
    )

    task_validate_filename >> task_process_file
