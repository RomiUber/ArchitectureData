
from airflow import DAG
from airflow.operators.python import PythonOperator 
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import requests
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from datetime import datetime, timedelta


default_args = {
    'owner': 'user',
    'start_date' : datetime(2024, 11, 8),
    'depends_on_past': False,
    'retries': 1 ,
    'retry_delay' : timedelta(minutes=5) 
}

dag = DAG(
    'netflix_data_pipeline',
    default_args=default_args,
    description='Pipeline de nettoyage et de transformation des données Netflix',
    schedule_interval= timedelta(days=1)
)

def ingest_static_data():
    # Charger les données statiques (Netflix movies) en utilisant un CSV
    df = pd.read_csv('/home/vboxuser/Downloads/archive/netflix_titles.csv')
    df.to_csv('/path/to/processed/netflix_titles.csv', index=False)

def ingest_dynamic_data():
    # Récupérer les données dynamiques depuis l'API Netflix
    url = "https://api.themoviedb.org/3/tv/popular"
    response = requests.get(url, params={'api_key': '607bcf001a678addfe26a33fc7c9b653'})
    data = response.json()['results']
    df = pd.DataFrame(data)
    df.to_csv('/home/vboxuser/ArchitectureData/netflix_tv_shows.csv', index=False)


start = EmptyOperator(
     task_id = 'start',
     dag=dag
)

ingest_static_task = PythonOperator(
    task_id='ingest_static_data',
    python_callable=ingest_static_data,
    dag=dag
)

ingest_dynamic_task = PythonOperator(
    task_id='ingest_dynamic_data',
    python_callable=ingest_dynamic_data,
    dag=dag
)

end = EmptyOperator(
     task_id = 'end',
     dag=dag
)

start >> ingest_static_task >> ingest_dynamic_task >> end