from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def process_file(**kwargs):
    # Access the filename parameter
    filename = kwargs['dag_run'].conf['filename']
    # Your file processing logic here
    print(f"Processing file: {filename}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG('file_processing_dag', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    process_task = PythonOperator(
        task_id='process_file',
        python_callable=process_file,
        provide_context=True,
    )

# Define a filename parameter to be passed to the DAG
filename_parameter = {'filename': 'example.txt'}

# Trigger the DAG with the filename parameter
dag_run_id = f"manual_trigger_{datetime.utcnow().isoformat()}"
dag.create_dagrun(run_id=dag_run_id, execution_date=datetime.utcnow(), conf=filename_parameter)
