from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

from datetime import datetime
from plugins.helper_functions import logger_setup
from plugins.helper_functions.utils import *
from plugins.helper_functions.extracting import extraction
import pandas as pd


# Universidad
university = 'GrupoJ_Pampa2'

# Configuracion del logger
logger = logger_setup.logger_creation(university)

# Conexion a s3
S3_ID = "aws_s3_bucket"

def calculateAge(birthDate:datetime):
    today = datetime.today()
    if today.month < birthDate.month:
        return today.year - birthDate.year - 1
    elif today.month == birthDate.month and today.day < birthDate.day:
        return today.year - birthDate.year - 1
    else: 
        return today.year - birthDate.year

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
    def transform():
        logger.info('Beggining transformation')
        try:
            df = pd.read_csv(f'./files/GrupoJ_Pampa2_select.csv')
            df.drop(df.columns[0], axis=1, inplace=True)

            df['university'] = df['university'].str.replace('-', ' ').str.strip().str.lower()
            df['career'] = df['career'].str.replace('-', ' ').str.strip().str.lower()
            df['first_name'] = df['first_name'].str.replace('-', ' ').str.strip().str.lower()
            df['email'] = df['email'].str.replace('-', '').str.strip().str.lower()

            df[['first_name', 'last_name']] = df['first_name'].str.split(" ", n = 1, expand=True)

            df['gender'] = df['gender'].map({'M': 'male', 'F': 'female'})

            df['birth_date'] = pd.to_datetime(df['birth_date'], format='%d/%m/%Y')
            df['inscription_date'] = pd.to_datetime(df['inscription_date'], format='%d/%m/%Y')

            df['age'] = df['birth_date'].map(calculateAge)
            df = df[df['age'].between(18, 100)]
            df.drop('birth_date', axis=1, inplace=True)

            dfCPostal = pd.read_csv('./assets/codigos_postales.csv')
            dfCPostal.rename(columns={'localidad':'location', 'codigo_postal': 'postal_code'}, inplace=True)
            dfCPostal['location'] = dfCPostal['location'].str.lower()
            df['location'] = df['postal_code'].map(dfCPostal.set_index('postal_code')['location'])

            df.to_csv(f'./datasets/{university}_process.txt', sep='\t', index=False)
        except Exception as err:
            logger.error(err)
            raise
        logger.info('Transformation finished without errors')

    load = LocalFilesystemToS3Operator(
        task_id='load',
        filename=f'./datasets/{university}_process.txt',
        dest_key=f'{university}_process.txt',
        dest_bucket='alkemy26',
        aws_conn_id=S3_ID,
        replace=True
    )
    
    extract() >> transform() >> load
