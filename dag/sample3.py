from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

def process_file(**kwargs):
    file_name = kwargs['dag_run'].conf['file_name']
    print(f"Processing file: {file_name}")
    # Your file processing logic goes here

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

with DAG('process_files_dag', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    # Define the task to process the file
    task_process_file = PythonOperator(
        task_id=f'process_file_{{{{ execution_date.strftime("%Y%m%d") }}}}_{{{{ ti.xcom_pull(task_ids="receive_file", key="file_counter") }}}}',
        python_callable=process_file,
        provide_context=True
    )
