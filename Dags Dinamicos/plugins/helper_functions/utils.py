from datetime import datetime, timedelta

# Diccionario con argumentos por defecto para pasar al DAG
default_args = {
    'owner': 'Fabian Manrique',
    'start_date': datetime(2022, 12, 1),
    'retries': 5,
    'retry_delay': timedelta(seconds=10),
    'description':"Dag para la extracción, transformación y cargue de la información de la facultad latinoamericana de Ciencias Sociales"
}