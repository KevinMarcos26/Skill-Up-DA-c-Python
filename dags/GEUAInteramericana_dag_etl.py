from datetime import timedelta, datetime
import pandas as pd
#ariflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
#helper functions imports
from plugins.helper_functions.logger_setup import logger_creation
from plugins.helper_functions.extracting import extraction

university = 'GrupoE_interamericana_universidad'

#logger config
logger = logger_creation(university)

# Connection with database
POSTGRES_ID = "alkemy_db"
S3_ID = "aws_s3_bucket"

def calculateAge(birthDate:datetime):
    today = datetime.today()
    if today.month < birthDate.month:
        return today.year - birthDate.year - 1
    elif today.month == birthDate.month and today.day < birthDate.day:
        return today.year - birthDate.year - 1
    else: 
        return today.year - birthDate.year

def extract():
    try:
        logger.info('Beggining extraction')
        extraction(university)
        logger.info('Extraction finished without errors')
    except Exception as err:
        logger.error(err)
        raise

def transform():
    logger.info('Beggining transformation')
    try:
        df = pd.read_csv(f'./files/{university}_select.csv')
        df.drop(df.columns[0], axis=1, inplace=True)

        df['university'] = df['university'].str.replace('-', ' ').str.strip().str.lower()
        df['career'] = df['career'].str.replace('-', ' ').str.strip().str.lower()
        df['first_name'] = df['first_name'].str.replace('-', ' ').str.strip().str.lower()
        df['location'] = df['location'].str.replace('-', ' ').str.strip().str.lower()
        df['email'] = df['email'].str.replace('-', '').str.strip().str.lower()

        df[['first_name', 'last_name']] = df['first_name'].str.split(" ", n = 1, expand=True)
        df['gender'] = df['gender'].map({'M': 'male', 'F': 'female'})

        df['birth_date'] = pd.to_datetime(df['birth_date'], format='%y/%b/%d')
        df['inscription_date'] = pd.to_datetime(df['inscription_date'], format='%y/%b/%d')

        df['age'] = df['birth_date'].map(calculateAge)
        df = df[df['age'] >= 18]
        df.drop('birth_date', axis=1, inplace=True)

        dfCPostal = pd.read_csv('./assets/codigos_postales.csv')
        dfCPostal.drop_duplicates(subset=['localidad'], inplace=True, keep='last')
        dfCPostal.rename(columns={'localidad':'location', 'codigo_postal': 'postal_code'}, inplace=True)
        dfCPostal['location'] = dfCPostal['location'].str.lower()
        df['postal_code'] = df['location'].map(dfCPostal.set_index('location')['postal_code'])

        df.to_csv(f'./datasets/{university}_process.txt', sep='\t', index=False)
    except Exception as err:
        logger.error(err)
        raise
    logger.info('Transformation finished without errors')

default_args = {
    'owner': 'Leandro Serra',
    'retries': '5',
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id = 'UAInteramericana_dag_etl',
    default_args= default_args,
    schedule= '@hourly',
    start_date= datetime(2022, 11, 28)
) as dag:
    extract = PythonOperator(
        task_id='Extract',
        python_callable=extract
    )
    transform = PythonOperator(
        task_id='transfrom',
        python_callable=transform
    )
    load = LocalFilesystemToS3Operator(
        task_id='load',
        filename=f'./datasets/{university}_process.txt',
        dest_key=f'{university}_process.txt',
        dest_bucket='alkemy26',
        aws_conn_id=S3_ID,
        replace=True
    )
    extract >> transform >> load