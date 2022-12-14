from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from plugins.helper_functions import logger_setup
from plugins.helper_functions.extracting import extraction
from plugins.helper_functions.utils import *
from plugins.helper_functions.loader import *
import pandas as pd
import numpy as np
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.operators.python import PythonOperator

# Universidad
university = 'GrupoJ_Villa_maria2'

# Configuracion del logger
logger = logger_setup.logger_creation(university)

# Conexion a s3
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

    # Transformación de Datos
    @task()
    def transform():

        logger.info("Transforming data")
        
        df = pd.read_csv(f'files/{university}_select.csv', index_col=0)
        df['university'] = df['university'].str.lower().str.replace('_',' ').str.strip()
        df['career'] = df['career'].str.lower().str.replace('_',' ').str.strip()
        df['first_name'] = df['first_name'].str.replace('_', ' ').str.strip().str.lower()
        df['email'] = df['email'].str.lower().str.replace('_',' ').str.strip()
        df['gender'] = df['gender'].map({'F': 'female', 'M': 'male'})
        df['inscription_date'] = df['inscription_date']
        df['fecha_nacimiento'] = pd.to_datetime(df['fecha_nacimiento'])
        df[['first_name', 'last_name']] = df['first_name'].str.split(" ", n = 1, expand=True)

        today = datetime.now()
        
        df['age'] = np.floor((today - df['fecha_nacimiento']).dt.days / 365)
        df['age'] = df['age'].apply(lambda x: x if (x > 18.0) and (x < 80) else -1)
        df['age'] = np.where(df['age']== -1, 21, df['age'])
        df['age'] = df['age'].astype(int)
        
        df = df.drop(columns='fecha_nacimiento')
        
        dfCod = pd.read_csv('./assets/codigos_postales.csv',sep=',')
        dfCod = dfCod.drop_duplicates(['localidad'], keep='last')
            
        dfC = df['postal_code']
        dfL = df['location']
        dfC = dfC.dropna()
        dfL = dfL.dropna()
        TamC = dfC.size
        TamL = dfL.size

        if TamL == 0:
                df = pd.merge(df,dfCod,left_on='postal_code',right_on='codigo_postal')
                del df['codigo_postal']
                del df['location']
                df = df.rename(columns={'localidad':'location'})

        if TamC == 0:
                df = pd.merge(df,dfCod,left_on='location',right_on='localidad')
                del df['localidad']
                del df['postal_code']
                df = df.rename(columns={'codigo_postal':'postal_code'})
        
        
        df = df[['university', 'career', 'inscription_date', 'first_name', 'last_name', 'gender', 'age', 'postal_code', 'location', 'email']]
        
        df.to_csv(f'./datasets/{university}_process.txt', sep='\t', index=False)
        
        logger.info("Data succesfully transformed")
        logger.info("Loading data to s3 bucket...")

    load = LocalFilesystemToS3Operator(
        task_id='load',
        filename=f'./datasets/{university}_process.txt',
        dest_key=f'{university}_process.txt',
        dest_bucket='alkemy26',
        aws_conn_id=S3_ID,
        replace=True
    )

    
    extract() >> transform() >> load

