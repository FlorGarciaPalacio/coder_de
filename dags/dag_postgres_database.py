from datetime import datetime, timedelta
from email.policy import default
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args={
    'owner': 'DavidBU',
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id='dag_con_conexion_postgres',
    description= 'Nuestro primer dag usando python Operator',
    start_date=datetime(2024,8,23),
    schedule_interval='0 0 * * *'
    ) as dag:
    task1= PostgresOperator(
        task_id='crear_tabla_postgres',
        postgres_conn_id= 'postgres_localhost',
        sql="""
            create table if not exists fin_mundo.fin_mundo(
                dt date,
                pais varchar(30)
            )
        """
    )
    task2 =PostgresOperator(
        task_id='insertar_en_tabla',
        postgres_conn_id= 'postgres_localhost',
        sql="""
        INSERT INTO fin_mundo.fin_mundo (dt, pais) VALUES 
        ('2025-12-12', 'Colombia'),
        ('2035-08-15', 'Brasil'),
        ('2030-09-21', 'Argentina'),
        ('2045-07-13', 'Chile'),
        ('2028-11-17', 'Ecuador'),
        ('2032-03-19', 'Peru'),
        ('2026-08-18', 'Uruguay'),
        ('2037-05-22', 'Paraguay'),
        ('2080-12-12', 'Venezuela'),
        ('2071-12-12', 'Mexico');

        """
    )
    task1 >> task2
