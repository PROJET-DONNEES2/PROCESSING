import pandas as pd
import datetime


def clean_data(filename, data, set):
    if set == "adzuna":
        # On extrait les données importantes (pole-emploi)
        results = data["results"]
        df = pd.json_normalize(results)

        # On enlève les colonnes inutiles
        class_columns = df.filter(like='__CLASS__').columns
        df = df.drop(columns=class_columns)
        df = df.drop(columns=['latitude', 'longitude', 'adref', 'redirect_url'])

        # On renomme quelques colonnes de notre dataframe
        df.rename(columns={"company.display_name": "company", "category.label": "category", "category.tag": "tag",
                           "location.display_name": "location", "location.area": "area"}, inplace=True)

        timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

        # Créer un fichier csv pour le dataframe fraichement nettoyé
        df.to_csv(f'/run/media/dinasoa/b4c37e5f-ad8f-4388-a46b-666dc08d1a12/COURS_HEI/DONNEES2/DONNEES2_PROJET/TRAITEMENT/jobs-etl//cleaned/adzuna/{filename}_{timestamp}.csv', index=False)

    if set == "pole":
        # On extrait les données importantes (pole-emploi)
        results = data["resultats"]
        df = pd.json_normalize(results)

        # On choisi seulement les colonnes nécéssaires
        selected_columns = ['id','typeContrat','dateCreation','intitule','appellationlibelle']

        df = df[selected_columns].rename(columns={"typeContrat":"contract_type", "dateCreation": "created", "intitule": "title"})

        timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

        # Créer un fichier csv pour le dataframe fraichement nettoyé
        df.to_csv(f'/run/media/dinasoa/b4c37e5f-ad8f-4388-a46b-666dc08d1a12/COURS_HEI/DONNEES2/DONNEES2_PROJET/TRAITEMENT/jobs-etl//cleaned/pole/{filename}_{timestamp}.csv', index=False)
