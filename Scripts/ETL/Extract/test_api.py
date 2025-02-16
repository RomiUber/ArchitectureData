import requests

api_key = "607bcf001a678addfe26a33fc7c9b653"
movie_title = "Inception"
url = f"http://api.themoviedb.org/3/search/movie?api_key={api_key}&query={movie_title}"

response = requests.get(url)

if response.status_code == 200:
    print(response.json())  # Affiche la réponse brute
    data = response.json()
    data.show()
else:
    print(f"Erreur: {response.status_code}")
