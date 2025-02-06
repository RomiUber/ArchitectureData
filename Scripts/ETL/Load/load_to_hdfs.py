from pyspark.sql import SparkSession

def load_to_hdfs(df_netflix, PATH_hdfs):
    df_netflix.write.parquet(PATH_hdfs, mode="overwrite")

    print('stockage reussi')
