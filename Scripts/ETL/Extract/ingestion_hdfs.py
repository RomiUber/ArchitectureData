from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def ingestion_hdfs(spark:SparkSession, PATH_hdfs):
    df2 = spark.read.parquet(PATH_hdfs, header= True, inferSchema= True)
    print("recupération réussie")
    df2.show(truncate=True)
    return df2
    