import pandas as pd
import os


def merge_df():
    # Définir les dossiers sources
    cleaned_adzuna = '/run/media/dinasoa/b4c37e5f-ad8f-4388-a46b-666dc08d1a12/COURS_HEI/DONNEES2/DONNEES2_PROJET/TRAITEMENT/jobs-etl//cleaned/adzuna'
    cleaned_pole = '/run/media/dinasoa/b4c37e5f-ad8f-4388-a46b-666dc08d1a12/COURS_HEI/DONNEES2/DONNEES2_PROJET/TRAITEMENT/jobs-etl//cleaned/pole'
    adzuna_list = []  # Liste pour les dataframes d'adzuna
    pole_list = []  # Liste pour les dataframes de pole

    for filename in os.listdir(cleaned_adzuna):
        if filename.endswith(".csv"):  # Vérifier que c'est un fichier csv
            file_path = os.path.join(cleaned_adzuna, filename)
            df = pd.read_csv(file_path)
            adzuna_list.append(df)  # Ajouter dans la liste

    # Maintenant nous allons joindre tous ces dataframes en un seul
    merged_adzuna = pd.concat(adzuna_list, ignore_index=True)

    for filename in os.listdir(cleaned_pole):
        if filename.endswith(".csv"):  # Vérifier que c'est un fichier csv
            file_path = os.path.join(cleaned_pole, filename)
            df = pd.read_csv(file_path)
            pole_list.append(df)  # Ajouter dans la liste

    # Maintenant nous allons joindre tous ces dataframes en un seul
    merged_pole = pd.concat(pole_list, ignore_index=True)

    # Créer un fichier csv pour le dataframe fraichement concaténé
    merged_adzuna.to_csv(f'/run/media/dinasoa/b4c37e5f-ad8f-4388-a46b-666dc08d1a12/COURS_HEI/DONNEES2/DONNEES2_PROJET/TRAITEMENT/jobs-etl//merged/adzuna-main.csv', index=False)

    # Créer un fichier csv pour le dataframe fraichement concaténé
    merged_pole.to_csv(f'/run/media/dinasoa/b4c37e5f-ad8f-4388-a46b-666dc08d1a12/COURS_HEI/DONNEES2/DONNEES2_PROJET/TRAITEMENT/jobs-etl//merged/pole-emploi-main.csv', index=False)


merge_df()
