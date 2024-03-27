from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import yaml

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 10),
    'retries': 1,
}

def read_yaml_and_get_secret():
    with open('/app/test.yaml', 'r') as file:
        data = yaml.safe_load(file)
        secrets = data.get('secrets', [])
        for secret in secrets:
            if secret.get('Vokey') == 'key_v':
                content_type = secret.get('content_type')
                path = secret.get('path')
                print(f"Retrieved secret: content_type={content_type}, path={path}")
                # Use the secret in your DAG logic

with DAG('read_yaml_dag', default_args=default_args, schedule_interval=None) as dag:
    get_secret_task = PythonOperator(
        task_id='get_secret',
        python_callable=read_yaml_and_get_secret
    )
