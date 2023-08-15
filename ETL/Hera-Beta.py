import pandas as pd
import os


def append_all():
    # This function is used to append adzuna and pole-emploi datas into one file

    # Define where are the files to merge
    current_directory = os.getcwd()
    appended_adzuna = os.path.join(current_directory, "merged/adzuna.csv")
    appended_pole_emploi = os.path.join(current_directory, "merged/pole-emploi.csv")

    adzuna_df = pd.read_csv(appended_adzuna)
    pole_emploi_df = pd.read_csv(appended_pole_emploi)

    df_merged = pd.concat([adzuna_df, pole_emploi_df], ignore_index=True)

    # Create destination folder
    destination_directory = "ready"
    destination_path = os.path.join(current_directory, destination_directory)
    if not os.path.exists(destination_path):
        os.mkdir(destination_path)

    # Store appended dataframes in csv file
    final_file_path = os.path.join(destination_path, "data.csv")
    df_merged.to_csv(final_file_path, index=False)

