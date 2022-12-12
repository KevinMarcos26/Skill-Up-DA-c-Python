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
    df['birth_date'] = pd.to_datetime(df['birth_date'])
    
    #Separar first_name y last_name
    for i, name in enumerate(list(df['first_name'])):
        name = name.split(sep=' ')
        df.loc[i,'first_name'] = name[0]
        df.loc[i,'last_name'] = name[1]

    today = dt.now()
    df['age'] = np.floor((today - df['birth_date']).dt.days / 365)
    df['age'] = df['age'].apply(lambda x: x if (x > 18.0) and (x < 80) else -1)
    df['age'] = np.where(df['age']== -1, 21, df['age'])
    df['age'] = df['age'].astype(int)
    
    df = df.drop(columns='birth_date')
    
    # Postal codes - Localidades
    postal_df = pd.read_csv(f"./assets/codigos_postales.csv")
    postal_df['localidad'] = postal_df['localidad'].str.lower()
    
    if df['postal_code'].isnull().values.any():
        df.drop(['postal_code'],axis=1,inplace=True)
        df = df.merge(postal_df, how='left', left_on='location', right_on='localidad')
        df.rename(columns = {'codigo_postal':'postal_code'}, inplace = True)
        df = df.drop_duplicates(['university', 'career', 'inscription_date', 'first_name', 'last_name', 'gender', 'age', 'location', 'email']).reset_index()
    else:
        df.drop(['location'], axis=1, inplace=True)
        df = df.merge(postal_df, how='left', left_on='postal_code', right_on='codigo_postal')
        df.rename(columns = {'localidad':'location'}, inplace = True) 

    logger.info('Guardando dataset en txt...')
    df = df[['university', 'career', 'inscription_date', 'first_name', 'last_name', 'gender', 'age', 'postal_code', 'location', 'email']]
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