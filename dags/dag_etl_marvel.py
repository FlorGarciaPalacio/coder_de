from datetime import datetime
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from APIS.functions import extract_response, transform_data, redshift_upload_data
from airflow.models.baseoperator import chain
import os
from dotenv import load_dotenv
from airflow.utils.email import send_email

load_dotenv()

default_args={
    'owner': 'FlorGP',
    'retries':2,
    'retry_delay': 2,
    'depends_on_past': False,
    'email':os.getenv('EMAIL'),
    'email_on_failure': True,
    'email_on_success': True,
    'email_on_retry': False,
    }

def extraer_xcom(endpoint, path, ds,ti):
    # Ejecuta la función original 
    cant_responses,json_path = extract_response(ds,endpoint, path)
    print(ds)
    #Establece el nombre de la tarea de acuerdo al resultado obtenido
    if cant_responses == 0: 
        response = 'no_data_to_transform'
    else:
        response = 'transformar-data-API'
        
    #Almacena en Xcom para enviar los datos a las tareas correspondientes
    ti.xcom_push(key='respuesta_api', value=response)
    ti.xcom_push(key='ruta-json',value=json_path)
    return 

def decide_run(ti):
    # Recupera el valor de XCom para determinar la tarea que sigue en el flujo
    extracted_data = ti.xcom_pull(task_ids='Extraer-data-API', key='respuesta_api')
    
    return extracted_data
    
def transform_xcom(path,ds,ti):
    
    #Toma el valor almacenado en el xcom de extraccion
    json_path = ti.xcom_pull(task_ids='Extraer-data-API', key='ruta-json')
    
    #guarda el valor de la ruta de los datos transformados
    new_path = transform_data(ds,json_path,path)
    
    #Almacena la ruta en Xcom 
    ti.xcom_push(key='transform-ruta-json',value=new_path)
    
    return 

def upload_xcom(ti):
    #sube los datos en redshift tomando la ruta de los datos transformados
    json_path = ti.xcom_pull(task_ids='transformar-data-API', key='transform-ruta-json')
    redshift_upload_data(json_path)
    return 

def success_status_email(ti,ds,email):
    task_status = ti.current_state()
    task_id = ti.task_id
    
    #armar email    
    subject = f"Airflow Task {task_id} {task_status}"
    body = f"La tarea {task_id} obtuvo un status: {task_status}.\n\n" \
           f"Dia de ejecucion de la tarea: {ds}\n" \
           f"Log URL: {ti.log_url}\n\n"

    to_email = email 

    send_email(to=to_email, subject=subject, html_content=body)
    
with DAG(
    default_args=default_args,
    dag_id='dag_api_marvel_endpoint_characters',
    description= 'Dag que extrae informacion de la API marvel endpoint characters',
    start_date=datetime.datetime(2024,8,1),
    schedule="0 0 * * *",
    catchup=True
    ) as dag:
        
    task_extract = PythonOperator(
        task_id='Extraer-data-API',
        python_callable=extraer_xcom,
        op_args=["characters",os.getcwd()]
        )
    
    branch_decide = BranchPythonOperator(
        task_id='decide_transform',
        python_callable=decide_run,
    )
    
    task_transform = PythonOperator(
        task_id='transformar-data-API',
        python_callable=transform_xcom,
        op_args=[os.getcwd()]
        )
    
    dummy_task = EmptyOperator(
        task_id = 'no_data_to_transform',
        on_success_callback=success_status_email
    )
    
    task_upload = PythonOperator(
        task_id='upload-data-API',
        python_callable=upload_xcom,
        on_success_callback=success_status_email
    )
    
    #Falta la task de redshift y la de mandar mail

task_extract >> branch_decide >> [task_transform, dummy_task] >> task_upload

