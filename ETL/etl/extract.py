import os
import json
from clean import clean_data


def retrieve_and_process_files():
    # Définir le dossier source
    adzunaset = '/run/media/dinasoa/b4c37e5f-ad8f-4388-a46b-666dc08d1a12/COURS_HEI/DONNEES2/DONNEES2_PROJET/TRAITEMENT/jobs-etl/srcset/adzuna'
    poleset = '/run/media/dinasoa/b4c37e5f-ad8f-4388-a46b-666dc08d1a12/COURS_HEI/DONNEES2/DONNEES2_PROJET/TRAITEMENT/jobs-etl/srcset/pole'

    # Itérer à partir du dossier source
    for filename in os.listdir(adzunaset):
        if filename.endswith('.json'): # Vérifier que c'est un fichier json
            file_path = os.path.join(adzunaset, filename)
            with open(file_path, 'r') as file:
                data = json.load(file)
                clean_data(filename,data,'adzuna') # Nettoyer le fichier

        # Itérer à partir du dossier source
    for filename in os.listdir(poleset):
        if filename.endswith('.json'):  # Vérifier que c'est un fichier json
            file_path = os.path.join(poleset, filename)
            with open(file_path, 'r') as file:
                data = json.load(file)
                clean_data(filename, data,'pole')  # Nettoyer le fichier


retrieve_and_process_files()
