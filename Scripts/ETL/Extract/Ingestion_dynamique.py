from pyspark.sql import SparkSession
import requests
import json
from pyspark.sql import Row

# Créer une session Spark
spark = SparkSession.builder.appName("MovieData").getOrCreate()

# Fonction pour récupérer les données d'un film depuis TMDB
def get_movie_data(movie_title, api_key):
    url = f"http://api.themoviedb.org/3/search/movie?api_key={api_key}&query={movie_title}"
    response = requests.get(url)
    data = response.json()
    return data

# Clé API de TMDB
api_key = "607bcf001a678addfe26a33fc7c9b653"

# Liste des films à rechercher
movies = ["Inception", "Interstellar", "Titanic", "Avatar", "The Godfather","The Dark Knight", "Pulp Fiction", "Fight Club", "Forrest Gump", "The Matrix", "Goodfellas", "The Lord of the Rings", "The Shawshank Redemption", "Gladiator", "Saving Private Ryan", "The Green Mile", "Schindler's List", "The Silence of the Lambs", "Se7en", "Django Unchained"]

# Liste pour stocker les résultats
movie_rows = []

# Récupérer les données pour chaque film
for movie in movies:
    movie_data = get_movie_data(movie, api_key)
    
    # Vérifier si des résultats existent
    if movie_data.get("results"):
        first_result = movie_data["results"][0]  # Prendre le premier résultat trouvé
        movie_rows.append(Row(
            title=first_result["title"],
            release_date=first_result.get("release_date", "N/A"),
            popularity=first_result.get("popularity", 0),
            vote_average=first_result.get("vote_average", 0),
            vote_count=first_result.get("vote_count", 0),
            adult=first_result.get("adult", False),
            genre_ids=first_result.get("genre_ids", [])
        ))
    else:
        # Si aucun résultat n'est trouvé, ajouter une ligne vide ou un message d'erreur
        movie_rows.append(Row(
            title=movie,
            release_date="N/A",
            popularity=first_result.get("popularity", 0),
            vote_average=0,
            vote_count=0,
            adult=first_result.get("adult", False),
            genre_ids=first_result.get("genre_ids", [])
        ))

# Créer un DataFrame Spark à partir des résultats récupérés
df_movies = spark.createDataFrame(movie_rows)

# Afficher le DataFrame avec les données des films
df_movies.show(truncate=True)

#Stockage dans hdfs 
#PATH_hdfs_dynamic= "hdfs://localhost:9000/user/ubuntu/df_netflix_dynamic.csv"

#df_movies.write.parquet(PATH_hdfs_dynamic, mode="overwrite")

#print("stockage_dynamique_to_hdfs  réussi")



