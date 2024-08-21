import os
from dotenv import load_dotenv

from InstagramAPI import *
import redshift_connector

# Carga las variables de entorno desde el archivo .env
load_dotenv()

access_token = os.getenv('ACCESS_TOKEN')
rd_user = os.getenv('USUARIO_REDSHIFT')
rd_pass = os.getenv('PASS_REDSHIFT')
rd_host = os.getenv('HOST_REDSHIFT')
rd_port = os.getenv('PORT_REDSHIFT')
rd_database = os.getenv('DATABASE_REDSHIFT')

#Conectarse a API instagram
instagram_api = InstagramAPI(access_token)

#Extract data
profile_fields = 'id,username,name,profile_picture_url,biography,website,followers_count,follows_count,media_count'
profile_data = instagram_api.api_profile_data(profile_fields)

print(profile_data)

#transform data
profile_df = instagram_api.dataframe_profile(profile_data)

print(profile_df)
  
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

#Insertar datos a profile table
sql_profile = """
INSERT INTO instagram_profile 
(profile_id,username,name_description,profile_picture_url, biography,
website,
followers_count,follows_count,media_count,execution_date)
VALUES (%s, %s, %s, %s, %s, %s, %s , %s, %s, %s)
"""
data_tuples = list(profile_df.itertuples(index=False, name=None))

print(data_tuples)

cursor.executemany(sql_profile, data_tuples)

conn.commit()

cursor.close()
conn.close()