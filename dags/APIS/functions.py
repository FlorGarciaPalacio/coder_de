from APIS.MarvelAPI import * 
import os
import json
from dotenv import load_dotenv
import redshift_connector
import pandas

def extract_response(exec_date,endpoint,path):
    """
    Extraer datos del endpoint que se elija de la API de Marvel
        
    https://developer.marvel.com/docs#!/public/getCreatorCollection_get_0
        
    Args:
        exec_date:string YYYY-MM-DD 
        api_endpoint: valores posibles en self.allowed_endpoints.
        path: path en el que se guarda el archivo json            
        
        Returns: 
               cant_responses: Cantidad de personajes que hay en la respuesta. Si es 0 quiere decir que no se encontro ningun caracter.
               json_path: el path del archivo json
        """
    
    # Carga las variables de entorno desde el archivo .env
    load_dotenv()
    
    #dia de la ejecucion
    presentDate = exec_date
    
    #Connect API
    api_public_key = os.getenv('MARVEL_PUBLIC_KEY')
    api_private_key = os.getenv('MARVEL_PRIVATE_KEY')

    api_conn = MarvelAPI(api_public_key,api_private_key)
    
    #extract json
    endpoint = endpoint
    api_response = api_conn.api_get(presentDate,endpoint)
    
    #save json as raw_data
    json_path = (
        f"{path}/raw_data/data_{presentDate}.json"
    )
    
    with open(json_path, "w+") as f:
        json.dump(api_response, f)
    
        print(f'Cantidad de personajes: {api_response['data']['total']}')
        
        cant_responses = api_response['data']['total']
    return cant_responses,json_path

def transform_data(exec_date, json_path, path):
    
    presentDate = exec_date
    
    raw_data_path = json_path
    
    with open(raw_data_path, "r") as json_file:
        my_data = json.load(json_file)

    new_dict = MarvelAPI.dict_characters(presentDate,my_data)

    json_path = (
        f"{path}/raw_data/new_data_{presentDate}.json"
    )
    
    with open(json_path, "w+") as f:
        json.dump(new_dict, f)

    return json_path

def redshift_upload_data(json_path):
    
    load_dotenv()
    rd_user = os.getenv('USUARIO_REDSHIFT')
    rd_pass = os.getenv('PASS_REDSHIFT')
    rd_host = os.getenv('HOST_REDSHIFT')
    rd_port = os.getenv('PORT_REDSHIFT')
    rd_database = os.getenv('DATABASE_REDSHIFT')
    
    raw_data_path = json_path
    with open(raw_data_path, "r") as json_file:
        my_data = json.load(json_file)
        
    data_frame = pandas.DataFrame(my_data)

    #Load Redshift
    conn = redshift_connector.connect(
        host=rd_host,
        database=rd_database,
        port=rd_port,
        user=rd_user,
        password=rd_pass
    )
  
    # Crear Cursor object
    cursor = conn.cursor()

    data_tuples = list(data_frame[['id', 'name', 'description', 'modified', 'thumbnail', 'resourceURI',
       'comics', 'series', 'stories', 'events', 'urls', 'exec_date']].itertuples(index=False, name=None))
    
    ids_to_delete = tuple(my_data['id'])
    
    #delete de los ids que tuvieron cambios en los datos
    sql_delete = f"""
    DELETE FROM marvel_characters
    WHERE id in {ids_to_delete}
    """
    cursor.execute(sql_delete)
    conn.commit()
    
    #Insertar datos a la table
    sql_insert = """
    INSERT INTO marvel_characters (
        id, character_name , character_description, date_modified, thumbnail, resourceURI,
        comics, series, stories, events, urls, exec_date)
    VALUES (%s, %s, %s, %s, %s, %s, %s , %s, %s, %s, %s, %s)
    """

    cursor.executemany(sql_insert, data_tuples)

    conn.commit()

    cursor.close()
    conn.close()
    return 