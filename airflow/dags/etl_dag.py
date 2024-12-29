from airflow import DAG
from airflow.operators.python import PythonOperator  # Correct import for Airflow 2.0+
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import requests
import pandas as pd


# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 24),
    'execution_timeout': timedelta(minutes=30)  # Temps limite d'exécution d'une tâche
}


# Task 1: Ingest static data
def ingest_static():
    spark = SparkSession.builder.appName('StaticIngestion').getOrCreate()
    df_statique = spark.read.csv('/home/vboxuser/Downloads/archive/netflix_titles.csv', header=True)
    df_statique.show()


# Task 2: Ingest dynamic data
def ingest_dynamic():
    url = 'https://api.themoviedb.org/3/movie/popular?api_key=607bcf001a678addfe26a33fc7c9b653'
    response = requests.get(url)
    dynamic_data = response.json()
    results = dynamic_data['results']  # Extracting the list of movies from the response
    df_dynamique = pd.DataFrame(results)  # Creating DataFrame from the 'results' key
    spark = SparkSession.builder.appName('DynamicIngestion').getOrCreate()
    spark_df_dynamique = spark.createDataFrame(df_dynamique)
    spark_df_dynamique.show()


# Define DAG
dag = DAG(
    'data_pipeline',  # DAG ID
    default_args=default_args,
    schedule_interval='@daily'
)


# Define tasks
t1 = PythonOperator(
    task_id='ingest_static',
    python_callable=ingest_static,
    dag=dag
)

t2 = PythonOperator(
    task_id='ingest_dynamic',
    python_callable=ingest_dynamic,
    dag=dag
)

# Set task dependencies
t1 >> t2