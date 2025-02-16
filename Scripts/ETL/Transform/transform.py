from pyspark.sql.functions import *


def delete_row(df_netflix_2):
    #suppression des espaces inutiles et transformation des pays en majuscule
    #filter(col(""))
    df_netflix_cleaned_1 = df_netflix_2.withColumn("country", trim(upper(col("country")))) 

    #remplacement des valeurs null par Inconnu dans country
    #df.na.drop(subset=["préciser les colonnes dans lesquels j'aimerais supprimer les valeurs vides "])
    df_netflix_cleaned_2 =  df_netflix_cleaned_1.fillna({'country': 'Inconnu'}) 

    #suppression des doublons
    df_netflix_cleaned_3=  df_netflix_cleaned_2.dropDuplicates()

    #conversion du texte en type date (to_date())
    #Formatage de la date dans un format personnalisé(date_format())
    df_netflix_cleaned =  df_netflix_cleaned_3.withColumn("date_added", date_format(col("date_added"), "yyyy-MM-dd"))
    #Extraction de l'année, du mois et du jour
    df_netflix_cleaned =  df_netflix_cleaned_3.withColumn("Année", year(col("date_added"))).withColumn("Mois", month(col("date_added"))).withColumn("Jour", dayofmonth
    (col("date_added")))

 
    return df_netflix_cleaned 
    

#main 
def cleaned_data(df_netflix_2):
    df_netflix_cleaned = delete_row(df_netflix_2)
  #  df_clean = delete_doublon(df_netflix_cleaned)
    print("nettoyage des données réussie")
    df_netflix_cleaned.show()
    return df_netflix_cleaned



 