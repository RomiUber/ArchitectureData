from pyspark.sql.functions import *

#suppression des lignes lorsque ces colonnes  sont vides 

def delete_empty_row(df_netflix_2):
    df_netflix_cleaned = df_netflix_2.dropna(how="all")  
    return df_netflix_cleaned 

#suppression des doublons
def delete_doublon(df_netflix_cleaned):
    df_no_duplicates = df_netflix_cleaned.dropDuplicates()
    return df_no_duplicates

#main 
def cleaned_data(df_netflix_2):
    df_netflix_cleaned = delete_empty_row(df_netflix)
    df_clean = delete_doublon(df_netflix_cleaned)
    print("nettoyage des données réussie")
    df_clean.show()
    return df_clean 



 