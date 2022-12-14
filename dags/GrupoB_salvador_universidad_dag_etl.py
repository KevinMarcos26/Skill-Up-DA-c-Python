import logging
from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from plugins.helper_functions import logger_setup
from plugins.helper_functions.extracting import extraction
from plugins.helper_functions.loader import *
from plugins.helper_functions.utils import *
import pandas as pd
import numpy as np
import csv
import boto3
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.operators.python import PythonOperator

# Universidad
university = 'GrupoB_salvador_universidad'

# Configuracion del logger
logger = logger_setup.logger_creation(university)

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

    @task()
    def transform():
        logging.info('transform started')
        
        try:
            with open('assets/codigos_postales.csv','r',encoding='utf-8') as f:
                cod_post_df = pd.read_csv(f)

            with open("files/GrupoB_salvador_universidad_select.csv") as f:
                df = pd.read_csv(f,index_col=[0])

            if df['postal_code'].isnull().values.any():
                df['location'] = df['location'].astype(str)
                df.location = df.location.str.replace('_', ' ')

                cod_post_df.rename(columns={"codigo_postal": "postal_code","localidad": "location",}, 
                inplace=True)

                df.drop(columns="postal_code",
                inplace=True)

                df = df.merge(cod_post_df, on="location", how="left")

            if df['location'].isnull().values.any():
                cod_post_df.rename(columns={"codigo_postal": "postal_code","localidad": "location",}, 
                inplace=True)

                df.drop(columns="location",
                inplace=True)

                df = df.merge(cod_post_df, on="postal_code", how="left")

            gender = {
                'f':'female',
                'm':'male',
                'F':'female',
                'M':'male'
            }

            df = df.replace({'gender': gender})

            df.career = df.career.str.strip()
            df.career = df.career.str.lower()

            df['fecha_nacimiento'] = pd.to_datetime(df['fecha_nacimiento'])
   
            today = datetime.now()
        
            df['age'] = np.floor((today - df['fecha_nacimiento']).dt.days / 365)
            df['age'] = df['age'].apply(lambda x: x if (x > 18.0) and (x < 80) else -1)
            df['age'] = np.where(df['age']== -1, 21, df['age'])
            df['age'] = df['age'].astype(int)
        
            df = df.drop(columns='fecha_nacimiento')

            df.university = df.university.astype(str)
            df.career = df.career.astype(str)
            df['first_name'] = df['first_name'].str.replace('-', ' ').str.strip().str.lower()
            df.location = df.location.astype(str)
            df.email = df.email.astype(str)

            df.university = df.university.str.replace('_', ' ')
            df.career = df.career.str.replace('_', ' ')

            df.university = df.university.str.lower()
            df.career = df.career.str.lower()
            df[['first_name', 'last_name']] = df['first_name'].str.split(" ", n = 1, expand=True)
            df.location = df.location.str.lower()
            df.email = df.email.str.lower()

            df = df.reindex(columns=[
                        "university",
                        "career",
                        "inscription_date",
                        "first_name",
                        "last_name",
                        "gender",
                        "age",
                        "postal_code",
                        "location", 
                        "email"])

            df.to_csv("datasets/GrupoB_salvador_universidad_process.txt",sep="\t",index=None)

            logging.info('txt file succesfully created')
        except Exception as e:
            logging.error(e)

    @task()
    def load(**kwargd):
        df_loader = Loader(university, logger)
        df_loader.to_load()

    
    extract() >> transform() >> load()

with DAG(
    "GrupoB_salvador_universidad_dag_etl",
    start_date=datetime(2022, 12, 4),
    schedule_interval="@hourly",
    default_args={
        "retries": 5
    },
    catchup=False,
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable = transform
    )

    load = PythonOperator(
        task_id="load",
        python_callable = load )
