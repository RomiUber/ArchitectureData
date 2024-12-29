from pyspark.sql import SparkSession
# Créer une session Spark
spark = SparkSession.builder.appName('IngestionCSV').getOrCreate()

# Lire un fichier CSV en utilisant Spark
df_statique = spark.read.csv('/home/vboxuser/Downloads/archive/netflix_titles.csv', header=True , inferSchema=True)

# Supprimer les doublons dans les données statiques
df_statique = df_statique.dropDuplicates()

# Supprimer les lignes avec des valeurs manquantes dans les deux DataFrames
df_static_cleaned = df_statique.na.drop()

# Afficher les premières lignes du DataFrame
df_statique.show()
df_static_cleaned.show()

#Fermer la session Spark
spark.stop()


