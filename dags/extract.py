from airflow.decorators import task


import pandas as pd
import os
from datetime import timedelta, datetime

from airflow.decorators import dag
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

import pandas as pd
import os



@task()
def extract():
    """Extract data from jobs.csv."""

    source_file_path = 'source/jobs.csv'
    extracted_folder_path = 'staging/extracted'

    # Créer le dossier staging/extracted s'il n'existe pas
    os.makedirs(extracted_folder_path, exist_ok=True)

    # Charger le DataFrame depuis le fichier CSV
    data = pd.read_csv(source_file_path)

    # Drop rows with missing values
    df = data.dropna()

    # Count missing values in each column
    # missing_values = df.isnull().sum()
    # print(missing_values)

    # Extraire la colonne 'context' et sauvegarder chaque élément dans un fichier texte
    for index, row in df.iterrows():
        context = str(row['context'])
        file_name = f"{extracted_folder_path}/extracted_{index}.txt"

        with open(file_name, 'w', encoding='utf-8') as file:
            file.write(context)