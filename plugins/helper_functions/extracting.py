from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

def extraction(university):
    # Lee el archivo .sql con el query para consultar los datos de la universidad
    with open(f'./include/{university}.sql','r', encoding='utf-8') as f:
        sql_script = f.read()

    # Inicia el hook a Postgres para conectar a la base de datos
    hook = PostgresHook(postgres_conn_id="alkemy_db")

    # Se crea el dataframe y lo exporta a la carpeta files
    df = hook.get_pandas_df(sql=sql_script)
    df.to_csv(f"./files/{university}_select.csv")