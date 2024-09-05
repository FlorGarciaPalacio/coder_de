from APIS.MarvelAPI import * 
import os
import json
from dotenv import load_dotenv



def extract_response(execution_date,endpoint,path):
    # Carga las variables de entorno desde el archivo .env
    load_dotenv()
    #Connect API
    api_conn = MarvelAPI(api_public_key = os.getenv('MARVEL_PUBLIC_KEY'),
              private_key= os.getenv('MARVEL_PRIVATE_KEY'))
    #extract json
    api_response = api_conn.api_get(modified_date=execution_date,api_endpoint=endpoint)
    
    #save json as raw_data
    json_path = (
        f"{path}/raw_data/data_{execution_date}.json"
    )
    
    with open(json_path, "w+") as f:
        json.dump(api_response, f)
    
    with open(json_path, "r") as j:
        my_data = json.load(api_response, j)
        
    return print({my_data['data']['total']})from APIS.MarvelAPI import * 
import os
import json
from dotenv import load_dotenv



def extract_response(endpoint,path):
    # Carga las variables de entorno desde el archivo .env
    load_dotenv()
    
    presentDate = datetime.datetime.now()
    presentDate = presentDate.strftime('%Y-%m-%d')
    
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
    return json_path

def transform_data(path):
    
    presentDate = datetime.datetime.now()
    presentDate = presentDate.strftime('%Y-%m-%d')
    
    raw_data_path = (
        f"{path}/raw_data/data_{presentDate}.json"
    )
    
    with open(raw_data_path, "r") as json_file:
        my_data = json.load(json_file)

    presentDate = datetime.datetime.now()
    presentDate = presentDate.strftime('%Y-%m-%d')
    new_dict = MarvelAPI.dict_characters(presentDate,my_data)

    json_path = (
        f"{path}/raw_data/new_data_{presentDate}.json"
    )
    
    with open(json_path, "w+") as f:
        json.dump(new_dict, f)

    return json_path
