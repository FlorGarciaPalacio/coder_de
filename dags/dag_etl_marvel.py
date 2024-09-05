from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from APIS.functions import *

default_args={
    'owner': 'FlorGP',
    'retries':5,
    'retry_delay': timedelta(seconds=30)
}

with DAG(
    default_args=default_args,
    dag_id='dag_api_marvel_endpoint_characters',
    description= 'Dag que extrae informacion de la API marvel endpoint characters',
    start_date=datetime(2024,9,1),
    schedule_interval=timedelta(days=1),
    catchup=False
    ) as dag:
    task_extract = PythonOperator(
        task_id='Extraer-data-API',
        python_callable=print(),
        op_args=['Hola Mundo']
        )

    task_extract