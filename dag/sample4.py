import re

# Define the filename pattern for validation
FILENAME_PATTERN = r'^[a-zA-Z0-9_.-]+$'

def process_file(filename, effective_date, **kwargs):
    if not re.match(FILENAME_PATTERN, filename):
        raise ValueError("Invalid filename. Filename must be alphanumeric and contain only dots, underscores, and hyphens.")

    print(f"Processing file: {filename} with effective date: {effective_date}")
    # Your file processing logic goes here

# Define the filename and effective_date dynamically using Jinja templating
filename = "{{ dag_run.conf['filename'] }}"
effective_date = "{{ dag_run.conf['effective_date'] }}"

with DAG('process_files_dag', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    # Define the task to process the file
    task_process_file = PythonOperator(
        task_id=f'process_file_{{{{ execution_date.strftime("%Y%m%d") }}}}_{filename}',
        python_callable=process_file,
        op_kwargs={'filename': filename, 'effective_date': effective_date},
        provide_context=True
    )
