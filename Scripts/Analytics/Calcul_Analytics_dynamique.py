from pyspark.sql import SparkSession
from pyspark.sql.functions import*
from Ingestion_dynamique2 import fetch_movie_data

#creation de la session spark
spark = SparkSession.builder.appName("Training").getOrCreate()

spark = spark.builder.appName("dynamique projet").getOrCreate()

df_dynamic = fetch_movie_data(movie_title, api_key)

df_dym = df_dynamic.groupBy("title").avg("vote_count")

df_dym.show()