import boto3
import os


def download_files(aws_access_key, aws_secret_key, region_name, folder, prefix):
    # This function aims at downloading files from a s3 bucket
    # @params
    # aws_access_key: the access key id
    # aws_secret_key: the secret access key
    # region_name: the region where the bucket is
    # folder: the name of the destination folder
    # prefix: the prefix of where in the bucket is the file to download

    session = boto3.Session(
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region_name
    )

    # Use the session to create a s3 resource object and getting access to the bucket
    s3_resource = session.resource('s3')
    my_bucket = s3_resource.Bucket('jobs-analysis')

    # Defining the destination directory
    current_directory = os.getcwd()
    destination_directory = folder
    destination_path = os.path.join(current_directory, destination_directory)
    os.mkdir(destination_path)

    objects = my_bucket.objects.filter(Prefix=prefix)
    for obj in objects:
        path, filename = os.path.split(obj.key)
        file_path = os.path.join(destination_path, filename)
        try:
            my_bucket.download_file(obj.key, file_path)
            print(f"Fichier '{obj.key}' téléchargé avec succès vers '{file_path}'.")
        except Exception as e:
            print(f"Erreur lors du téléchargement de '{obj.key}': {e}")