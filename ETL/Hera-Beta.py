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

    columns_to_fill = ['salary_max', 'salary_min']

    df_merged[columns_to_fill] = df_merged[columns_to_fill].fillna(0)

    fill_values = {col: "N/A" for col in df_merged.columns}

    df_merged = df_merged.fillna(fill_values)

    # Create destination folder
    destination_directory = "ready"
    destination_path = os.path.join(current_directory, destination_directory)
    if not os.path.exists(destination_path):
        os.mkdir(destination_path)

    # Clean and store appended dataframes in csv file
    final_file_path = os.path.join(destination_path, "data.csv")
    df_merged.to_csv(final_file_path, index=False)


if __name__ == '__main__':
    append_all()