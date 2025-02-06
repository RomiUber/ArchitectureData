from pyspark.sql import SparkSession
from pyspark.sql.functions import*


#creation de la session spark

PATH_hdfs_clean= "hdfs://localhost:9000/user/ubuntu/df_netflix_clean.csv"

PATH_hdfs_insight1 = "hdfs://localhost:9000/user/ubuntu/Insights/Insight1.csv"

spark = SparkSession.builder.appName("Training").getOrCreate()

df3 = spark.read.parquet(PATH_hdfs_clean, header= True, inferSchema= True)

#df3.printSchema()

#Insight 1 

df3.select("show_id","type","date_added","release_year", "duration").show(truncate=False)



df3.write.parquet(PATH_hdfs_insight1, mode="overwrite")

print("stockage_insight1_réussi")





