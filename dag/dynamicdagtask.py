from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def process_file(filename, processing_date, **kwargs):
    # Your file processing logic goes here
    print(f"Processing file: {filename} on {processing_date}")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 1),
    'catchup': False
}

dag = DAG(
    'dynamic_dag_based_on_filename_and_date',
    default_args=default_args,
    schedule_interval=None,
)

# Define a function to create tasks dynamically
def create_task(filename, processing_date):
    task_id = f'process_{filename.split(".")[0]}_{processing_date}'  # Generating task_id based on filename and date
    return PythonOperator(
        task_id=task_id,
        python_callable=process_file,
        op_kwargs={'filename': filename, 'processing_date': processing_date},  # Pass filename and date as parameters
        provide_context=True,
        dag=dag,
    )

# Define the function to generate tasks dynamically based on filenames
def generate_tasks(**kwargs):
    dag_run_conf = kwargs.get('dag_run').conf
    filenames = dag_run_conf.get('filenames', [])
    processing_date = datetime.now().strftime('%Y-%m-%d')

    for filename in filenames:
        create_task(filename, processing_date)

# Define the task to trigger task generation dynamically
generate_tasks_task = PythonOperator(
    task_id='generate_tasks',
    python_callable=generate_tasks,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
generate_tasks_task
