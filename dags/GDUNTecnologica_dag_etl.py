from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
from plugins.helper_functions import logger_setup
from plugins.helper_functions.extracting import extraction
from plugins.helper_functions.loader import Loader
from plugins.helper_functions.transformer import Transformer
import pandas as pd
import numpy as np

# Universidad
university = 'GrupoD_tecnologica_universidad'
date_format = '%Y-%m-%d'

# Conexion AWS
aws_conn = 'aws_s3_bucket'
dest_bucket = 'alkemy26'

# Default args de airflow
default_args = {
    'owner': 'Gastón Orphant',
    'start_date': datetime(2022, 12, 1),
    'retries': 5,
    'retry_delay': timedelta(seconds=10),
    'description':'Dag para la extracción, transformación y carga de la información de la Universidad Nacional Tecnologica'
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

def transform():
    try:
        logger.info('Inicio de proceso de transformación')
        df = pd.read_csv(f'files/{university}_select.csv')

        #Separar first_name y last_name
        for i, name in enumerate(list(df['first_name'])):
            name = name.split(sep=' ')
            df.loc[i,'first_name'] = name[0]
            df.loc[i,'last_name'] = name[1]

        # GenderParsing
        df['gender'] = df['gender'].str.lower()
        df.gender.replace(['m', 'f'], ['male', 'female'], inplace=True)

        # Formato de fecha
        columns_to_transform = ['inscription_date', 'birth_date']
        for column in columns_to_transform:
            df[column] = pd.to_datetime(df[column], format=date_format)
            df.style.format({column: lambda t: t.strftime("%d-%m-%Y")}) 
            if date_format == '%y-%b-%d':
                df[column].where(df[column] < pd.Timestamp.now(), df[column] - pd.DateOffset(years=100), inplace=True)
        
        # Calculo de edad        
        today = pd.Timestamp.now()      
        df['age'] = df['birth_date'].apply(
                lambda x: today.year - x.year - 
                ((today.month, today.day) < (x.month, x.day))
                )
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
        df.to_csv(f'./datasets/{university}_process.txt', index=False, sep='\t')
        logger.info('Dataset guardado correctamente.')
    except Exception as e:
        logger.info('ERROR al transformar los datos')
        logger.error(e)



# Definimos el DAG
with DAG(f'{university}_dag_etl',
         default_args=default_args,
         catchup=False,
         schedule_interval='@hourly'
         ) as dag:      
    
    extraccion = PythonOperator(task_id = 'extraccion', python_callable=extract)
    
    transformacion = PythonOperator(task_id = 'transformacion', python_callable=transform)

    @task()
    def load(**kwargd):
        df_loader = Loader(university, logger=logger, S3_ID=aws_conn, dest_bucket=dest_bucket)
        df_loader.to_load()
    
    extraccion >> transformacion >> load()