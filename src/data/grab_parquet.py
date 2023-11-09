from datetime import datetime

from minio import Minio
import urllib.request
import pandas as pd
import sys
from bs4 import BeautifulSoup
import requests


def main():
    grab_data()
    

def grab_data() -> None:
    url = "https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')

        # Trouver tous les liens avec le texte "Yellow Taxi Trip Records"
        links = soup.find_all('a', string="Yellow Taxi Trip Records")

        for link in links:
            file_url = link['href']
            file_name = file_url.split("/")[-1]

            # Extraction de la date du nom du fichier
            try:
                # Le format de la date est 'YYYY-MM'
                date_str = file_name.split('_')[-1].split('.')[0]
                file_date = datetime.strptime(date_str, '%Y-%m')

                # Définir la plage de dates
                start_date = datetime(2018, 1, 1)
                end_date = datetime(2023, 8, 31)

                if start_date <= file_date <= end_date:
                    file_path = f'../../data/raw/{file_name}'

                    # Télécharger le fichier
                    file_response = requests.get(file_url)

                    if file_response.status_code == 200:
                        with open(file_path, 'wb') as file:
                            file.write(file_response.content)

                        print(f"Le fichier {file_name} a été téléchargé avec succès.")
                    else:
                        print(
                            f"Échec du téléchargement du fichier {file_name}. Code d'état : {file_response.status_code}")
            except ValueError:
                # La date dans le nom du fichier n'a pas le bon format, ou ce n'est pas un fichier de données
                print(
                    f"Le fichier {file_name} n'a pas une date dans un format reconnaissable ou ce n'est pas un fichier de données.")

    else:
        print(f"Échec du chargement de la page. Code d'état : {response.status_code}")


def grab_last_data() -> None:



def write_data_minio():
    """
    This method put all Parquet files into Minio
    Ne pas faire cette méthode pour le moment
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = "NOM_DU_BUCKET_ICI"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")

if __name__ == '__main__':
    sys.exit(main())
