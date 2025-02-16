from pyspark.sql import SparkSession
import requests
import json
spark = SparkSession.builder.appName("MovieData").getOrCreate()
api_key = "607bcf001a678addfe26a33fc7c9b653"
movie_title = "Inception"
# Récupère les données d'un film depuis l'API TMDB et les charge dans un DataFrame Spark
def fetch_movie_data(movie_title, api_key):
    url = f"http://api.themoviedb.org/3/search/movie?api_key={api_key}&query={movie_title}"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json().get("results", [])  # Extraire uniquement les résultats
        if data:
            df_dynamique = spark.createDataFrame(data)
            print("Récupération API réussie et chargement dans le DataFrame")
            df_dynamique.show(20)
            df_dynamique.printSchema()
            return df_dynamique
        else:
            print("Aucun résultat trouvé pour le film.")  
            return None
    else:
        print("Erreur lors de la récupération des données")
        return None
# Exécuter la fonction
df_dynamique1 = fetch_movie_data(movie_title, api_key)
if df_dynamique1 is not None:
    PATH_hdfs_dynamique = "hdfs://localhost:9000/user/ubuntu/data_dynamique.parquet"
    df_dynamique1.write.mode("overwrite").parquet(PATH_hdfs_dynamique)
    print("Stockage HDFS réussi")
