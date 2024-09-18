import httpx
import time
from hashlib import md5
import datetime
import re

class MarvelAPIError(Exception):
    def __init__(self, error_code, message):
        self.error_code = error_code  # Almacena el código de error
        self.message = message  # Almacena el mensaje de error
        super().__init__(f"Error {self.error_code}: {self.message}")  # Inicializa la clase base

    def __str__(self):
        return f"MarvelAPIError (Code {self.error_code}): {self.message}"
    
class MarvelAPI:
    def __init__(self,public_key,private_key):
        
        ts = str(int(time.time()))
        to_hash= f'{ts}{private_key}{public_key}'.encode('utf-8')
        md5_key = md5(to_hash).hexdigest()
        
        self.public_key = public_key
        self.hashed_key = md5_key
        self.ts = ts
        self.allowed_endpoints = {'characters', 'comics', 'creators', 'events','series', 'stories'}
        
    def api_get(self,modified_date:str,api_endpoint:str):
        """
        Extraer datos del endpoint que se elija de la API de Marvel
        
        https://developer.marvel.com/docs#!/public/getCreatorCollection_get_0
        
        Args:
            modified_date:string YYYY-MM-DD 
            api_endpoint: valores posibles en self.allowed_endpoints.
            
        
        Returns: 
               response_json: json de la respuesta de la API
        """
        
        if api_endpoint not in self.allowed_endpoints:
            raise ValueError(f"El endpoint '{api_endpoint}' no es válido. Los endpoints permitidos son: {self.allowed_endpoints}")
        
        # Realiza una solicitud GET para obtener información los personajes
        url = f'https://gateway.marvel.com/v1/public/{api_endpoint}'
        
        response = httpx.get(url, params={
            'modifiedSince': modified_date,
            'limit':50,
            'ts': self.ts,
            'apikey': self.public_key,
            'hash': self.hashed_key
            })
        
        if response.status_code == 200: 
            response_json = response.json()               
        else:
            error_message = response.text
            error_code = response.status_code
            raise MarvelAPIError(error_code, error_message)
        
        return response_json

    @staticmethod
    def dict_characters(execution_date: datetime , api_response: dict):
        """Funcion para trasformar el json resultado de la API a Dataframe para preparar los datos para exportar a redshift.
        Args:
            execution_date (datetime): datetime of the execution
            response_profile (dict): Responce from the characters api endpoint

        Returns:
            profile_df: Panda DataFrame con las columnas resultantes
        """
        if api_response['data']['total']==0:
            api_result='Empty Data'
            
        else:
            #Copiar keys del dictionario
            nuevo_dic = {key: list() for key in api_response['data']['results'][0]}
            #agregar clave para fecha ejecucion
            nuevo_dic['exec_date']=list()

            ts = str(execution_date) #execution date
            keys_mantain = ['name', 'description', 'modified', 'resourceURI'] #keys que tienen los mismos datos
            comp_keys = ['comics', 'series', 'stories', 'events'] #keys de datos con el mismo formato
            pattern = r'\d+$' #patern para extraer ids de las urls

            #generar dict para cargar en redshift
            for i in range(len(api_response['data']['results'])):
                nuevo_dic['exec_date'].append(ts)
                nuevo_dic['id'].append(int(api_response['data']['results'][i]['id']))
                nuevo_dic['thumbnail'].append(str(api_response['data']['results'][i]['thumbnail']['path']+'.'+api_response['data']['results'][i]['thumbnail']['extension']))
                nuevo_dic['urls'].append(str(api_response['data']['results'][i]['urls']))
                
                for key_name in keys_mantain:
                    nuevo_dic[key_name].append(str(api_response['data']['results'][i][key_name]))

                for key_name in comp_keys:
                    list_ids = list()
                    for item_num in range(len(api_response['data']['results'][i][key_name]['items'])):        
                        match = re.search(pattern,api_response['data']['results'][i][key_name]['items'][item_num]['resourceURI'])
                        list_ids.append(match.group())
                
                    nuevo_dic[key_name].append(str({
                        'total_amount': api_response['data']['results'][i][key_name]['available'],
                        'ids_top_20':list_ids
                        }))
                 
            api_result = nuevo_dic                   
        
        return api_result