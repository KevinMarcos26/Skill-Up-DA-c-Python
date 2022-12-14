from airflow import DAG
from airflow.decorators import task

from plugins.helper_functions import logger_setup
from plugins.helper_functions.utils import *
from plugins.helper_functions.transformer import Transformer
from plugins.helper_functions.extractor import Extractor
from plugins.helper_functions.loader import Loader


# Universidad
university = 'GrupoB_comahue_universidad'
db_conn = 'alkemy_db'
date_format = '%Y-%m-%d'
aws_conn = 'aws_s3_bucket'
dest_bucket = 'alkemy26'

# Configuracion del logger
logger = logger_setup.logger_creation(university)

# Definimos el DAG
with DAG(f'{university}_dag_etl',
         default_args=default_args,
         catchup=False,
         schedule_interval='@hourly'
         ) as dag:      
    
    # Extracción de Datos
    @task()
    def extract(**kwargd):
        df_extractor = Extractor(university, logger=logger, db_conn=db_conn)
        df_extractor.to_extract()
    
    # Transformación de Datos
    @task()
    def transform(**kwargd):
        df_transformer = Transformer(university, logger=logger, date_format=date_format)
        df_transformer.to_transform()

    @task()
    def load(**kwargd):
        df_loader = Loader(university, logger=logger, S3_ID=aws_conn, dest_bucket=dest_bucket)
        df_loader.to_load()
    
    extract() >> transform() >> load()