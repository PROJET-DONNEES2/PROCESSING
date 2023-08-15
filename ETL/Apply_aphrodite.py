import os
import json
from Aphrodite import clean_data


def apply_aphrodite():
    # This function apply the clean_data function on all downloaded files

    # Define where are the files to clean
    current_directory = os.getcwd()
    adzunaset = os.path.join(current_directory, "adzuna")
    poleset = os.path.join(current_directory, "pole-emploi")

    # Clean all detected files
    for filename in os.listdir(adzunaset):
        if filename.endswith('.json'):  # Verify that it is a json file
            file_path = os.path.join(adzunaset, filename)
            with open(file_path, 'r') as file:
                data = json.load(file)
                clean_data(filename.split(".")[0], data, 'adzuna')  # Clean datas

    for filename in os.listdir(poleset):
        if filename.endswith('.json'):
            file_path = os.path.join(poleset, filename)
            with open(file_path, 'r') as file:
                data = json.load(file)
                clean_data(filename.split(".")[0], data, 'pole')

if __name__ == '__main__':
    apply_aphrodite()