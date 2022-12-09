from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task

from datetime import datetime as dt
from datetime import timedelta
from plugins.helper_functions import logger_setup
from plugins.helper_functions.extracting import extraction
from plugins.helper_functions.transformer import Transformer
from plugins.helper_functions.loader import Loader
import pandas as pd
import numpy as np

# Universidad
university = 'GrupoC_palermo_universidad'
date_format = '%Y-%m-%d'

# Conexion AWS
aws_conn = 'aws_s3_bucket'
dest_bucket = 'alkemy26'

# Default args de airflow
default_args = {
    'owner': 'Gastón Orphant',
    'start_date': dt(2022, 12, 1),
    'retries': 5,
    'retry_delay': timedelta(seconds=10),
    'description':'Dag para la extracción, transformación y carga de la información de la Universidad Nacional de Palermo'
}

# Configuracion del logger
logger = logger_setup.logger_creation(university)

# Funciones de python

#Extraccion de datos

def extract():
    logger.info('Inicio de proceso de extracción')
    try:
        extraction(university)
        logger.info("Se creo el csv con la información de la universidad.")
    except Exception as e:
        logger.error(e)

#Transformación de datos

def transform():
    logger.info('Iniciando proceso de transformación')
    df = pd.read_csv(f'files/{university}_select.csv', index_col=0)
    df['university'] = df['university'].str.lower().str.replace('_',' ').str.strip()
    df['career'] = df['career'].str.lower().str.replace('_',' ').str.strip()
    df['first_name'] = df['first_name'].str.lower().str.replace('_',' ').str.strip().str.replace('(m[r|s]|[.])|(\smd\s)', '', regex=True)
    df['email'] = df['email'].str.lower().str.replace('_',' ').str.strip()
    df['gender'] = df['gender'].map({'f': 'female', 'm': 'male'})
    df['inscription_date'] = df['inscription_date']
    df['birth_date'] = pd.to_datetime(df['birth_date'])
    
    today = dt.now()
    
    df['age'] = np.floor((today - df['birth_date']).dt.days / 365)
    df['age'] = df['age'].apply(lambda x: x if (x > 18.0) and (x < 80) else -1)
    df['age'] = np.where(df['age']== -1, 21, df['age'])
    df['age'] = df['age'].astype(int)
    
    df = df.drop(columns='birth_date')
    
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
    
    
    df = df[['university', 'career', 'inscription_date', 'first_name', 'gender', 'age', 'postal_code', 'location', 'email']]
    
    df.to_csv(f'./datasets/{university}_process.txt', sep='\t', index=False)
    
    logger.info("Datos transformados satisfactoramiente")


# Definimos el DAG
with DAG(f'{university}_dag_etl',
         default_args=default_args,
         catchup=False,
         schedule_interval='@hourly'
         ) as dag:      
    
    extraccion = PythonOperator(task_id = 'extraccion', python_callable=extract)
    
    transformacion = PythonOperator(task_id = 'transform', python_callable=transform)

    @task()
    def load(**kwargd):
        df_loader = Loader(university, logger=logger, S3_ID=aws_conn, dest_bucket=dest_bucket)
        df_loader.to_load()

    extraccion >> transformacion >> load()