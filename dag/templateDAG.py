from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import json

def execute_etl_pipeline(pipeline_config):
    # Implement ETL logic here
    print("Executing ETL Pipeline...")
    print(json.dumps(pipeline_config, indent=2))

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
}

dag = DAG('etl_pipeline',
          default_args=default_args,
          schedule_interval='@once')

with dag:
    for pipeline_file in os.listdir("pipelines"):
        with open(f"pipelines/{pipeline_file}") as file:
            pipeline_config = json.load(file)
            task = PythonOperator(
                task_id=f'etl_{pipeline_config["pipeline_name"]}',
                python_callable=execute_etl_pipeline,
                op_kwargs={'pipeline_config': pipeline_config},
            )
