import requests
import datetime
import pandas as pd

class InstagramAPIError(Exception):
    def __init__(self, error_code, message):
        self.error_code = error_code  # Almacena el código de error
        self.message = message  # Almacena el mensaje de error
        super().__init__(f"Error {self.error_code}: {self.message}")  # Inicializa la clase base

    def __str__(self):
        return f"InstagramAPIError (Code {self.error_code}): {self.message}"
    
class InstagramAPI:
    def __init__(self,api_token):
        self.api_token = api_token
        
    def api_profile_data(self, profile_fields):
        """
        Extraer datos del perfil de instagram con endpoint me de API instagram. 
        
        profile_fields: str con los campos que se desean obtener, separados por coma.
                        Posibles campos: id,username,name,profile_picture_url,biography,website,followers_count,follows_count,media_count
        
        Returns: 
               response_json: json de la respuesta o text de la response si se genera un error
        """
        # Realiza una solicitud GET para obtener información del perfil
        profile_fields = profile_fields
        url = f"https://graph.instagram.com/v9.0/me?access_token={self.api_token}&fields={profile_fields}"
        response_perfil = requests.get(url)
        
        if response_perfil.status_code == 200 : 
            response_json = response_perfil.json()
        else:
            error_message = response_perfil.text
            error_code = response_perfil.status_code
            raise InstagramAPIError(error_code, error_message)

        return response_json

    def api_posts_data(self,media_fields,media_amount):
        """
        Extraer datos de las publicaciones de instagram con endpoint me/media de API instagram. 
        
        Args:
        
        media_fields: str con los campos que se desean obtener, separados por coma.
            Posibles campos: id,caption,media_type,media_url,thumbnail_url,permalink,timestamp,children,like_count,comments_count
        
        media_amount: Cantidad de publicaciones que se quieren extraer, ejemplo si se selecciona 10 se extraen las 10 ultimas.
                    El limite de extraccion es 100 publicaciones
            
        Return:
               data_list: Lista con la informacion de la key 'data' de la respuesta de la API
        """
        # Realiza una solicitud GET para obtener información de los posts del perfil
        media_fields = media_fields
        url_posts = f"https://graph.instagram.com/v9.0/me/media?access_token={self.api_token}&fields={media_fields}"
        
        response_posts = requests.get(url_posts)
        
        if response_posts.status_code == 200 : 
            response_json = response_posts.json()
            data_list = response_posts.json()['data']
            
            if media_amount > 100 :
                media_amount = 100
        
            while 'paging' in response_json and 'next' in response_json['paging'] and len(data_list) <= media_amount:
                next_url = response_posts.json()['paging']['next']
                response = requests.get(next_url)
                new_data = response.json()['data']
                data_list.extend(new_data)
  
        else:
            error_message = f"Error endpoint /me/media: {response_posts.text}"
            error_code = response_posts.status_code
            raise InstagramAPIError(error_code, error_message)     
        
        data_list = data_list[0:media_amount] 
  
        return data_list
    
    @staticmethod
    def dataframe_profile(response_profile: dict):
        """Funcion para trasformar el json resultado de la API a Dataframe para preparar los datos para exportar a redshift.
        Args:
            response_profile (dict): respuesta json de la API endpoint /me

        Returns:
            profile_df: Panda DataFrame con las columnas resultantes
        """
        presentDate = datetime.datetime.now()
        profile_df = pd.DataFrame([response_profile])
            
            # Agregar columna con fecha de ejecucion
        profile_df['execution_date']=presentDate
        profile_df['execution_date'] = profile_df['execution_date'].dt.strftime('%Y-%m-%d') 
            
        #Lista de posibles campos y tipo de datos
        fields_dtypes = {'id':'int64', #BIGINT
                            'username':'str', #VARCHAR
                            'name':'str', #VARCHAR
                            'profile_picture_url':'str', #VARCHAR
                            'biography':'str', #VARCHAR
                            'website':'str', #VARCHAR
                            'followers_count':'int', #VARCHAR
                            'follows_count':'int', #VARCHAR
                            'media_count':'int' #VARCHAR
                            }
            
        #convertir tipos de datos para matchear con redshift
        
        for column, dtype in fields_dtypes.items():
            if column in profile_df.columns:
                profile_df[column] = profile_df[column].astype(dtype)
            
            # Reemplazar caracteres de espacios
        if 'biography' in profile_df.columns:
                profile_df['biography'] = profile_df['biography'].apply(lambda x: x.replace('\n.',' ').replace('\n',' '))
            
        return profile_df
    
    @staticmethod
    def dataframe_posts(data_media: list):
        """Funcion para trasformar el resultado de la API a Dataframe para preparar los datos para exportar a redshift.
        Args:
            data_media (list): Lista con el resultado de la API endpoint me/media

        Returns:
            media_df: Panda DataFrame copipn las columnas resultantes
        """
        media_df = pd.DataFrame(data_media)
        
        for i in range(1,len(data_media)):
            media_df.loc[i]=data_media[i]

        presentDate = datetime.datetime.now()
        media_df['execution_date']=presentDate
        # Convertir 'execution_date' a DATE (solo fecha)
        media_df['execution_date'] = media_df['execution_date'].dt.strftime('%Y-%m-%d') 
            
        # Transformar los datos para matchear con redshift
        fields_dtypes = {'id':'int64', #BIGINT
                         'caption': 'str', #SUPER
                         'media_type':'str', #VARCHAR
                         'media_url': 'str', #VARCHAR
                         'thumbnail_url':'str', #VARCHAR
                         'permalink':'str', #VARCHAR
                         'children':'str', #VARCHAR
                         'like_count':'int64', #INTEGER,
                         'comments_count':'int64' #INTEGER
                         }
                
        for column, dtype in fields_dtypes.items():
            if column in media_df.columns:
                media_df[column] = media_df[column].astype(dtype)

        # Convertir 'timestamp' a TIMESTAMP
        if 'timestamp' in media_df.columns:
            media_df['timestamp'] = pd.to_datetime(media_df['timestamp'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
            media_df['date_filter'] = pd.to_datetime(media_df['timestamp'], errors='coerce').dt.strftime('%Y-%m-%d') 
            
        if 'caption' in media_df.columns:
            media_df['caption']=media_df['caption'].apply(lambda x: x.replace('\n.',' ').replace('\n',' '))

        return media_df