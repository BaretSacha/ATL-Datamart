#!/usr/bin/python3

import csv
import os
import sys

current_script_path = os.path.abspath(__file__)
root_path = os.path.abspath(os.path.join(current_script_path, "..", "..", ".."))

def main():
    updateCSV()

def updateCSV() -> None:
    # Chemin vers le répertoire contenant les fichiers Parquet
    repertoire_parquet = f'{root_path}/data/raw'
    
    # print(current_script_path)
    # print(__file__)

    # Liste des fichiers Parquet dans le répertoire
    fichiers_parquet = [f for f in os.listdir(repertoire_parquet) if f.endswith('.csv')]

    # Boucle pour convertir chaque fichier Parquet en CSV
    for fichier_parquet in fichiers_parquet:
        chemin_parquet = os.path.join(repertoire_parquet, fichier_parquet)
        replace_first_row(chemin_parquet)

        # exit()
      
def replace_first_row(csv_file):
    # Read the existing CSV file
    with open(csv_file, 'r', newline='') as file:
        reader = csv.reader(file)
        rows = list(reader)

    if (len(rows) == 0):
        print('empty file')
        # close the file
        file.close()
        return
    # Replace the first row with the new data
    if (rows[0][18] == 'airport_fees'):
        print('already replaced')
        return
    rows[0][18] = 'airport_fee'

    # Write the modified data back to the CSV file
    with open(csv_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(rows)  

if __name__ == '__main__':
    sys.exit(main())
