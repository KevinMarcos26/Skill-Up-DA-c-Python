from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task

from datetime import datetime as dt
from datetime import timedelta
from plugins.helper_functions import logger_setup
from plugins.helper_functions.extracting import extraction
from plugins.helper_functions.loader import Loader
import pandas as pd

# Universidad
university = 'GrupoC_palermo_universidad'

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
    logger.info('Inicio de proceso de transformación')
    DataLocationPostal_Code = open('./assets/codigos_postales.csv', 'r')  #abrir csv codigos postales y guardarlos
    data=pd.read_csv(f'files/{university}_select.csv', index_col=0)
    dataLocation = pd.read_csv(DataLocationPostal_Code)    # leer y guardar en DataFrame los codigos postales y guardarlos en dataframe
    dataLocation['location'] = dataLocation['location'].str.lower()  # Transformo los valores de localidad de codigos postales

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
        lambda x: dt.date.today().year - x.year - ((dt.date.today().month, dt.date.today().day) < (x.month, x.day)))
        data['age'] = age

    if 'location' in data.columns:  # Agrego las columnas postal_code o location
        data = data.merge(dataLocation, how='inner', on='location', copy=False, suffixes=('X', ''))
    else:
        data['postal_code'] = data['postal_code'].astype(int).replace('.0','')
        data = data.merge(dataLocation, how='inner', on='postal_code', copy=False, suffixes=('X', ''))
    
    data = data.loc[:,~data.columns.str.contains('X')]# drop columna vacía
    logger.info('Guardando dataset en txt...')
    data.to_csv(f'./datasets/{university}_process.txt', header=None, index=None, sep=' ', mode='a')
    logger.info('Dataset guardado correctamente.')  
    return data        

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