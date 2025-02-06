from pyspark.sql import SparkSession

def load_to_hdfs_clean(df_clean, PATH_hdfs_clean):
    df_clean.write.parquet(PATH_hdfs_clean, mode="overwrite")

    print('stockage_cleaned_to_hdfs reussi')