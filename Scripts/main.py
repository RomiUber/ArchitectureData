from pyspark.sql import SparkSession
from pyspark.sql.functions import*
from ETL.Extract.ingestion_csv import ingestion_data
from ETL.Load.load_to_hdfs import load_to_hdfs
from ETL.Extract.ingestion_hdfs import ingestion_hdfs
from ETL.Transform.transform import cleaned_data
from ETL.Load.load_to_hdfs_clean import load_to_hdfs_clean


#creation de la session spark

spark = SparkSession.builder.appName("Training").getOrCreate()

#lien des dataset 

PATH_netflix_titles_csv = "/home/ubuntu/BIG_DATA_PROJECT/Data"
PATH_hdfs = "hdfs://localhost:9000/user/ubuntu/netflix_titles.csv"
PATH_hdfs_clean= "hdfs://localhost:9000/user/ubuntu/df_netflix_clean.csv"




#Liste des fonctions 
#df_netflix = ingestion_data(spark,PATH_netflix_titles_csv) 
#load_to_hdfs(df_netflix, PATH_hdfs)
#df_netflix_2 = ingestion_hdfs(spark,PATH_hdfs)
#df_netflix_clean = cleaned_data(df_netflix_2)
#load_to_hdfs_clean(df_netflix_clean,PATH_hdfs_clean)
