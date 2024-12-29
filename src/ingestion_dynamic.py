
import requests
import pandas as pd
from pyspark.sql import SparkSession



# URL de l'API publique
url = 'https://api.themoviedb.org/3/movie/popular?api_key=607bcf001a678addfe26a33fc7c9b653'

# Récupérer les données de l'API
response = requests.get(url)

if response.status_code == 200:
    data = response.json()
else:
    print(f"Erreur lors de la récupération des données : {response.status_code}")
    exit()

# Convertir les données API en DataFrame Pandas
df_dynamique_pd = pd.DataFrame(data)

# Créer une session Spark
spark = SparkSession.builder.appName('IngestionAPI').getOrCreate()

# Convertir le DataFrame Pandas en DataFrame Spark
df_dynamique_spark = spark.createDataFrame(df_dynamique_pd)

# Supprimer les doublons dans les données API
df_api_cleaned = df_dynamique_spark.dropDuplicates()


# Supprimer les lignes avec des valeurs manquantes dans les deux DataFrames
df_api_cleaned_dynamique = df_api_cleaned.na.drop()

# Afficher les premières lignes du DataFrame
df_api_cleaned_dynamique.show()

# Fermer la session Spark
spark.stop()





