from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
import requests
import pandas as pd
from datetime import timedelta


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 20),
    'execution_timeout': timedelta(minutes=30)  # Temps limite d'exécution d'une tâche
}



def ingest_static():
    spark = SparkSession.builder.appName('StaticIngestion').getOrCreate()
    df_statique = spark.read.csv('/home/vboxuser/Downloads/archive/netflix_titles.csv', header=True)
    df_statique.show()

def ingest_dynamic():
    url = 'https://api.themoviedb.org/3/movie/popular?api_key=607bcf001a678addfe26a33fc7c9b653'
    response = requests.get(url)
    dynamic_data = response.json()
    df_dynamique = pd.DataFrame(dynamic_data)
    spark = SparkSession.builder.appName('DynamicIngestion').getOrCreate()
    spark_df_dynamique = spark.createDataFrame(df_dynamique)
    spark_df_dynamique.show()

dag = DAG('data_pipeline', default_args=default_args, schedule_interval='@daily')

t1 = PythonOperator(task_id='ingest_static', python_callable=ingest_static, dag=dag)
t2 = PythonOperator(task_id='ingest_dynamic', python_callable=ingest_dynamic, dag=dag)

t1 >> t2  # Define task dependencies


