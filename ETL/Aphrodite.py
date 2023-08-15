import pandas as pd
import datetime
import os


def clean_data(filename, data, set):
    # This function is used to clean the datas
    # @params
    # filename: the name of the file
    # data: the datas stored in a file
    # set: to define if the file contains adzuna or pole-emploi datas
    current_directory = os.getcwd()
    clean_data_path = os.path.join(current_directory, "cleaned-datas")
    if set == "adzuna":
        # Extract important datas
        results = data["results"]
        df = pd.json_normalize(results)

        # Drop not important columns
        class_columns = df.filter(like='__CLASS__').columns
        df = df.drop(columns=class_columns)
        df = df.drop(columns=['latitude', 'longitude', 'adref', 'redirect_url'])

        # Renaming some columns
        df.rename(columns={"company.display_name": "company", "category.label": "category", "category.tag": "tag",
                           "location.display_name": "location", "location.area": "area"}, inplace=True)

        # Create the destination directory of the cleaned file
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        if not os.path.exists(clean_data_path):
            os.mkdir(clean_data_path)
        adzuna_clean_data_path = os.path.join(clean_data_path, "adzuna")
        if not os.path.exists(adzuna_clean_data_path):
            os.mkdir(adzuna_clean_data_path)
        cleaned_filename = f'{filename}_{timestamp}.csv'
        cleaned_file_path = os.path.join(adzuna_clean_data_path, cleaned_filename)

        # Store cleaned datas into a new file
        df.to_csv(cleaned_file_path, index=False)

    if set == "pole":
        # Extract important datas
        results = data["resultats"]
        df = pd.json_normalize(results)

        # Drop not important columns
        selected_columns = ['id', 'typeContrat', 'dateCreation', 'intitule', 'appellationlibelle']

        df = df[selected_columns].rename(
            columns={"typeContrat": "contract_type", "dateCreation": "created", "intitule": "title"})

        # Create the destination directory of the cleaned file
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        if not os.path.exists(clean_data_path):
            os.mkdir(clean_data_path)
        pole_emploi_clean_data_path = os.path.join(clean_data_path, "pole-emploi")
        if not os.path.exists(pole_emploi_clean_data_path):
            os.mkdir(pole_emploi_clean_data_path)
        cleaned_filename = f'{filename}_{timestamp}.csv'
        cleaned_file_path = os.path.join(pole_emploi_clean_data_path, cleaned_filename)

        # Store cleaned datas into a new file
        df.to_csv(cleaned_file_path,index=False)
