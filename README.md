# Proyecto Final Coder House - Data Engineer - Comision 61895

Este proyecto fue realizado como parte del trabajo final del Curso de Data Engineer de Coder House. Para el mismo se utilizo la [API de Marvel](https://developer.marvel.com/documentation/apiresults) (Data provided by Marvel. 2024 MARVEL)

El proyecto consiste en la consulta diaria del endpoint Characters de la API, con el objetivo de actualizar la base de datos de personajes para que refleje los ultimos cambios realizados. La base de datos utilizada sera Redshift.

El repositorio posee un archivo .yml que permitira la ejecucion de docker compose de airflow 2.10

Para poder realizar la ejecucion del archivo docker y el dag se deberan seguir los siguientes pasos:

1. Crear la tabla marvel_charcters por medio del [SQL script](https://github.com/FlorGarciaPalacio/coder_de/blob/ProyectoFinal_CoderDE/sql/Create_marvel_character_table.sql) ubicado en la carpeta sql

2. Completar los archivos .env ubicados en el root principal y en la carpeta ./dags.

3. En la terminal ejecutar el codigo bash que creara las carpetas necesarias para que airflow funcione:
 ```bash
mkdir -p ./logs ./plugins ./config
 ```
3. Inicializar la database de airflow
 ```bash
docker compose up airflow-init
 ```
4. Inicializar Airflow
```bash
docker compose up 
 ```
