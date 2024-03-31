from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

# Define a function to process a file
def process_file(file_name, **kwargs):
    print(f"Processing file: {file_name}")
    # Your file processing logic goes here

# Define a function to determine the next task based on the filename
def decide_next_task(file_name, **kwargs):
    if file_name.startswith("condition"):
        return "task_matching_condition"
    else:
        return "task_not_matching_condition"

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

# Define the DAG
with DAG('process_files_dag', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    for i in range(1, 101):
        file_name = f'file_{i}.txt'

        # Define tasks dynamically for each file
        task_process_file = PythonOperator(
            task_id=f'process_file_{datetime.now().strftime("%Y%m%d")}_{i}',
            python_callable=process_file,
            op_kwargs={'file_name': file_name},
            provide_context=True
        )

        task_decide_next_task = BranchPythonOperator(
            task_id=f'decide_next_task_{datetime.now().strftime("%Y%m%d")}_{i}',
            python_callable=decide_next_task,
            op_kwargs={'file_name': file_name},
            provide_context=True
        )

        task_matching_condition = PythonOperator(
            task_id=f'task_matching_condition_{datetime.now().strftime("%Y%m%d")}_{i}',
            python_callable=lambda: print("Task matching condition"),
        )

        task_not_matching_condition = PythonOperator(
            task_id=f'task_not_matching_condition_{datetime.now().strftime("%Y%m%d")}_{i}',
            python_callable=lambda: print("Task not matching condition"),
        )

        # Define task dependencies
        task_process_file >> task_decide_next_task
        task_decide_next_task >> [task_matching_condition, task_not_matching_condition]
