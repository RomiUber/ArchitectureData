from pyspark.sql import SparkSession

# Créer une session Spark
spark = SparkSession.builder.appName('IngestionCSV').getOrCreate()

# Lire un fichier CSV en utilisant Spark
df_statique = spark.read.csv('/home/vboxuser/Downloads/archive/netflix_titles.csv', header=True , inferSchema=True)

# Afficher les premières lignes du DataFrame
df_statique.show()

#Fermer la session Spark
spark.stop()


