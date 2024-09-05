from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from APIS.functions import extract_response,transform_data
import os

default_args={
    'owner': 'FlorGP',
    'retries':2,
    'retry_delay': 2
}

with DAG(
    default_args=default_args,
    dag_id='dag_api_marvel_endpoint_characters',
    description= 'Dag que extrae informacion de la API marvel endpoint characters',
    start_date=datetime(2024,9,1),
    schedule="0 0 * * *",
    catchup=True
    ) as dag:
    task_extract = PythonOperator(
        task_id='Extraer-data-API',
        python_callable=extract_response,
        op_args=["characters",os.getcwd()]
        )
    task_transform = PythonOperator(
        task_id='transformar-data-API',
        python_callable=transform_data,
        op_args=[os.getcwd()]
        )
task_extract >> task_transform
