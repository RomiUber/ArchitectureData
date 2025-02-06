from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def ingestion_data(spark:SparkSession, PATH_netflix_titles_csv):
    df1  = spark.read.csv(PATH_netflix_titles_csv, header= True, inferSchema= True)
    print("importation réussie")
    df1.show(truncate=True)
    return df1
    