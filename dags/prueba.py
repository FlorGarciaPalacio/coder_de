import os
from dotenv import load_dotenv
from APIS.MarvelAPI import *

# Carga las variables de entorno desde el archivo .env
load_dotenv()

#variables Marvel API
api_public_key = os.getenv('MARVEL_PUBLIC_KEY')
api_private_key = os.getenv('MARVEL_PRIVATE_KEY')

api = MarvelAPI(api_public_key,api_private_key)

#variables redshift

rd_user = os.getenv('USUARIO_REDSHIFT')
rd_pass = os.getenv('PASS_REDSHIFT')
rd_host = os.getenv('HOST_REDSHIFT')
rd_port = os.getenv('PORT_REDSHIFT')
rd_database = os.getenv('DATABASE_REDSHIFT')

#consultar api
endpoint = 'characters'
date='2013-09-18'
api_response = api.api_get(date,endpoint)

ts = datetime.datetime.now() #execution date
dict_transform = api.dict_characters(ts,api_response)

print(dict_transform)