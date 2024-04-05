from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def process_file(source_filename):
    # Your file processing logic goes here
    print(f"Processing file: {source_filename}")

# Define a function to create tasks dynamically
def create_task(task_id, source_filename):
    return PythonOperator(
        task_id=task_id,
        python_callable=process_file,
        op_args=[source_filename],
        dag=dag,
    )

# Define a list of source filenames
source_filenames = ['file1.csv', 'file2.csv', 'file3.csv']

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 1),
    'catchup': False
}

dag = DAG(
    'dynamic_dag_based_on_filename',
    default_args=default_args,
    schedule_interval=None,
)

# Create tasks dynamically based on source filenames
for filename in source_filenames:
    task_id = f'process_{filename.split(".")[0]}'  # Generating task_id based on filename
    create_task(task_id, filename)

# Set task dependencies (if any)
# For example:
# task1 >> task2

