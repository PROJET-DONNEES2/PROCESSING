import pandas as pd
import os


def append_same_sources_df():
    # This function is used to append datas in multiple files into one, for the same source

    # Define where are the files to merge
    current_directory = os.getcwd()
    cleaned_adzuna = os.path.join(current_directory, "cleaned-datas/adzuna")
    cleaned_pole = os.path.join(current_directory, "cleaned-datas/pole-emploi")
    adzuna_list = []  # Dataframe list for adzuna
    pole_list = []  # Dataframe list for adzuna

    for filename in os.listdir(cleaned_adzuna):
        if filename.endswith(".csv"):  # Verify that it's a csv file
            file_path = os.path.join(cleaned_adzuna, filename)
            df = pd.read_csv(file_path)
            adzuna_list.append(df)

    # Append Adzuna dataframes
    merged_adzuna = pd.concat(adzuna_list, ignore_index=True)

    for filename in os.listdir(cleaned_pole):
        if filename.endswith(".csv"):
            file_path = os.path.join(cleaned_pole, filename)
            df = pd.read_csv(file_path)
            pole_list.append(df)

    # Append Pole Emploi Dataframes
    merged_pole = pd.concat(pole_list, ignore_index=True)

    # Create destination folder
    destination_directory = "merged"
    destination_path = os.path.join(current_directory, destination_directory)
    if not os.path.exists(destination_path):
        os.mkdir(destination_path)

    # Store appended dataframes in csv file
    adzuna_merged_file_path = os.path.join(destination_path, "adzuna.csv")
    merged_adzuna.to_csv(adzuna_merged_file_path, index=False)

    pole_emploi_merged_file_path = os.path.join(destination_path, "pole-emploi.csv")
    merged_pole.to_csv(pole_emploi_merged_file_path, index=False)

