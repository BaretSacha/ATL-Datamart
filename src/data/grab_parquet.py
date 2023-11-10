from datetime import datetime

from minio import Minio
import pandas as pd
import sys
from bs4 import BeautifulSoup
import requests
import os


def main():
    deleteParquet()

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


def grab_last_data_from_last_month() -> None:
    url = "https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

    current_year = datetime.now().year
    last_month = datetime.now().month - 1 if datetime.now().month > 1 else 12

    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')

        links = soup.find_all('a', string="Yellow Taxi Trip Records")

        # Télécharger le fichier du dernier mois complet de l'année en cours
        file_downloaded = False
        for link in links:
            file_url = link['href']
            file_name = file_url.split("/")[-1]
            if f"{current_year}-{str(last_month).zfill(2)}" in file_name:
                file_path = f'../../data/raw/{file_name}'
                file_response = requests.get(file_url)
                if file_response.status_code == 200:
                    with open(file_path, 'wb') as file:
                        file.write(file_response.content)
                    print(
                        f"Le fichier {file_name} a été téléchargé avec succès pour {current_year}-{str(last_month).zfill(2)}.")
                    file_downloaded = True
                    break  # Sortir de la boucle après le téléchargement réussi
                else:
                    print(f"Échec du téléchargement du fichier {file_name}. Code d'état : {file_response.status_code}")

        if not file_downloaded:
            print(f"Aucun fichier trouvé pour {current_year}-{str(last_month).zfill(2)}.")

    else:
        print(f"Échec du chargement de la page. Code d'état : {response.status_code}")

def grab_last_data_dispo() -> None:
    # URL de la page contenant les liens de téléchargement
    url = "https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

    # Obtenir l'année en cours
    current_year = datetime.now().year

    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')

        # Trouver tous les liens avec le texte "Yellow Taxi Trip Records"
        links = soup.find_all('a', string="Yellow Taxi Trip Records")

        # Inverser l'ordre des liens pour commencer par le plus récent
        links = reversed(links)

        # Parcourir les liens pour trouver le dernier fichier de l'année en cours
        file_downloaded = False
        for link in links:
            file_url = link['href']
            file_name = file_url.split("/")[-1]

            # Vérifier si le fichier correspond à l'année en cours
            if str(current_year) in file_name:
                file_path = f'../../data/raw/{file_name}'
                file_response = requests.get(file_url)
                if file_response.status_code == 200:
                    with open(file_path, 'wb') as file:
                        file.write(file_response.content)
                    # Extraction de la date du nom du fichier pour affichage
                    date_str = file_name.split('_')[-1].split('.')[0]
                    print(
                        f"Le fichier {file_name} correspondant à {date_str} a été téléchargé avec succès car c'est le dernier mois disponible de l'année {current_year} présent sur le site.")
                    file_downloaded = True
                    break  # Sortir de la boucle après le téléchargement réussi
                else:
                    print(f"Échec du téléchargement du fichier {file_name}. Code d'état : {file_response.status_code}")

        if not file_downloaded:
            print(f"Aucun fichier trouvé pour l'année {current_year}.")

    else:
        print(f"Échec du chargement de la page. Code d'état : {response.status_code}")




def updateCSV() -> None:
    # Chemin vers le répertoire contenant les fichiers Parquet
    repertoire_parquet = '../../data/raw'

    # Liste des fichiers Parquet dans le répertoire
    fichiers_parquet = [f for f in os.listdir(repertoire_parquet) if f.endswith('.parquet')]

    # Boucle pour convertir chaque fichier Parquet en CSV
    for fichier_parquet in fichiers_parquet:
        chemin_parquet = os.path.join(repertoire_parquet, fichier_parquet)

        # Charger le fichier Parquet
        df = pd.read_parquet(chemin_parquet)

        # Construire le chemin pour le fichier CSV
        chemin_csv = os.path.splitext(chemin_parquet)[0] + '.csv'

        # Convertir le DataFrame en fichier CSV
        df.to_csv(chemin_csv, index=False)

        print(f'Conversion terminée : {fichier_parquet} -> {chemin_csv}')


def deleteParquet() -> None:
    # Chemin vers le répertoire contenant les fichiers Parquet
    repertoire_parquet = '../../data/raw'

    # Liste des fichiers Parquet dans le répertoire
    fichiers_parquet = [f for f in os.listdir(repertoire_parquet) if f.endswith('.parquet')]

    # Boucle pour supprimer chaque fichier Parquet
    for fichier_parquet in fichiers_parquet:
        chemin_parquet = os.path.join(repertoire_parquet, fichier_parquet)

        # Supprimer le fichier Parquet
        os.remove(chemin_parquet)

        print(f'Suppression terminée : {fichier_parquet}')




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
