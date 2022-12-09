from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

from datetime import datetime
from plugins.helper_functions import logger_setup
from plugins.helper_functions.utils import *
from plugins.helper_functions.extracting import extraction
import pandas as pd


# Universidad
university = 'GrupoI_Jujuy2'

# Configuracion del logger
logger = logger_setup.logger_creation(university)

# Conexion a S3
S3_ID = "aws_s3_bucket"

# Definimos el DAG
with DAG(f'{university}_dag_etl',
         default_args=default_args,
         catchup=False
         ) as dag:      
    
    # Extracción de Datos
    @task()
    def extract():
        logger.info('Inicio de proceso de extracción')
        try:
            extraction(university)
            logger.info("Se creo el csv con la información de la universidad")

        except Exception as e:
            logger.error(e)
            raise
    
    # Transformación de Datos
    @task()
    def transform() -> pd.DataFrame:
        DataLocationPostal_Code = open('./assets/codigos_postales.csv', 'r')  #abrir csv codigos postales y guardarlos
        data_path=open(f'./files/{university}_select.csv', encoding="utf8")


        data=pd.read_csv(data_path)
        data.drop(data.columns[0], axis=1, inplace=True) #drop a columna con indices
        dataLocation = pd.read_csv(DataLocationPostal_Code)    # leer y guardar en DataFrame los codigos postales y guardarlos en dataframe
        dataLocation = dataLocation.rename(columns={'localidad': 'location',
                                                    'codigo_postal': 'postal_code'})  # Transformo los nombres de la columnas del cdv de codigos_postales
        dataLocation['location'] = dataLocation[
            'location'].str.lower()  # Transformo los valores de localidad de codigos postales


        for col in data.columns:  # convertimos todo a minusculas
            if data[col].dtype == 'object':
                data[col] = (data[col].str.lower()
                             .str.replace('-', ' ')  # reemplazamos los guiones internos por espacios para separar palabras
                             .str.replace('_', ' ')  # reemplazamos los guiones internos por espacios para separar palabras
                             .str.lstrip('-')  # sacamos guiones al inicio
                             .str.rstrip('-'))  # sacamos guiones al final

        data.first_name = (data.first_name
                      .str.split('-')  # separo por -
                      .apply(lambda x: " ".join(x))  # uno la lista con espacios
                      .str.split('\.', n=1)  # separo todas aquellas ocurrencias que contengan . como dr., ms., etc
                      .apply(
            lambda x: x[1] if len(x) >= 2 else x)  # me quedo con el segundo elemento si la lista tiene un largo mayor a 2
                      .apply(
            lambda x: "".join(x) if type(x) == list else x)  # convierto a string los elementos de tipo lista
                      .str.strip()
                      )
        data.gender = data.loc[:, ['gender']].replace('m', 'male').replace('f', 'female')

        nom_apell = (data.first_name
                     .str.rsplit(expand=True, n=1)
                     .rename(columns={0: 'first_name', 1: 'last_name'}))

        data[['first_name','last_name']] = nom_apell
        data.inscription_date = pd.to_datetime(data.inscription_date, infer_datetime_format=True)

        for data in [data]:
            data.age = pd.to_datetime(data.age , infer_datetime_format=True)
            age = data.age.apply(
            lambda x: datetime.today().year - x.year - ((datetime.today().month, datetime.today().day) < (x.month, x.day)))
            data['age'] = age

        if 'location' in data.columns:  # Agrego las columnas postal_code o location

            data = data.merge(dataLocation, how='inner', on='location', copy=False, suffixes=('X', ''))
        else:
            data['postal_code'] = data['postal_code'].astype(int).replace('.0','')
            data = data.merge(dataLocation, how='inner', on='postal_code', copy=False, suffixes=('X', ''))



        data = data.loc[:,~data.columns.str.contains('X')]# drop columna vacía

        data.to_csv(f'./datasets/{university}_process.txt', index=False, sep='\t')

    load = LocalFilesystemToS3Operator(
        task_id='load',
        filename=f'./datasets/{university}_process.txt',
        dest_key=f'{university}_process.txt',
        dest_bucket='alkemy26',
        aws_conn_id=S3_ID,
        replace=True
    )
    
    extract() >> transform() >> load
