import os
import json
from ETL.Aphrodite import clean_data


def retrieve_and_process_files():
    # Définir le dossier source
    adzunaset = '/run/media/onitsiky/664797E9304437B4/hei/Semestre 4/me/Données2/PROCESSING/ETL/srcset/adzuna'
    poleset = '/run/media/onitsiky/664797E9304437B4/hei/Semestre 4/me/Données2/PROCESSING/ETL/srcset/pole'

    # Itérer à partir du dossier source
    for filename in os.listdir(adzunaset):
        if filename.endswith('.json'):  # Vérifier que c'est un fichier json
            file_path = os.path.join(adzunaset, filename)
            with open(file_path, 'r') as file:
                data = json.load(file)
                clean_data(filename.split(".")[0], data, 'adzuna')  # Nettoyer le fichier

        # Itérer à partir du dossier source
    for filename in os.listdir(poleset):
        if filename.endswith('.json'):  # Vérifier que c'est un fichier json
            file_path = os.path.join(poleset, filename)
            with open(file_path, 'r') as file:
                data = json.load(file)
                clean_data(filename, data, 'pole')  # Nettoyer le fichier


if __name__ == '__main__':
    retrieve_and_process_files()
