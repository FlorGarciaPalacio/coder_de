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
        
    return print({my_data['data']['total']})