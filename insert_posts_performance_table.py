import os
from dotenv import load_dotenv

from InstagramAPI import *
import redshift_connector

print("start")
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
media_fields = 'id,caption,media_type,media_url,thumbnail_url,permalink,timestamp,children,like_count,comments_count'
media_amount = 50
posts_data = instagram_api.api_posts_data(media_fields = media_fields ,media_amount = media_amount)

#Extract data
profile_fields = 'id'
profile_data = instagram_api.api_profile_data(profile_fields)

profile_id = profile_data['id']

#transform data
post_df = instagram_api.dataframe_posts(posts_data)

post_df['account_id'] = int(profile_id)

print(post_df)
  
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

posts_performance_sql = """
INSERT INTO instagram_media
(media_id, media_type, timestamp_publish,
like_count, comments_count, execution_date, account_id)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

performance_tuples = list(post_df[['id', 'media_type', 'timestamp',
'like_count', 'comments_count', 'execution_date', 'account_id']].itertuples(index=False, name=None))

cursor.executemany(posts_performance_sql, performance_tuples)

posts_info_sql = """
INSERT INTO instagram_media_posts
(media_id, media_type, timestamp_publish,account_id,caption,media_url,permalink,carrousel_children,thumbnail_url,
 execution_date)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

#update last day posts
posts_tuples = list(post_df[['id', 'media_type', 'timestamp','account_id',
                             'caption','media_url','permalink','children',
                             'thumbnail_url', 'execution_date']][post_df['date_filter']==post_df['execution_date']].itertuples(index=False, name=None))

cursor.executemany(posts_info_sql, posts_tuples)

conn.commit()

cursor.close()
conn.close()