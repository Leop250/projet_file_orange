from google.cloud import bigquery
import pandas as pd
import os


csv_folder = r"C:\HETIC\2025-2026\Projet_File_0range\projet_file_orange\fichier_csv"
project_id = "projet-fil-orange-477313"
dataset_id = "air_quality_europe_monthly_avg"
keyfile_path = r"/projet_file_orange/Load\keyfile.json"


client = bigquery.Client.from_service_account_json(keyfile_path, project=project_id)


dataset_ref = client.dataset(dataset_id)
dataset = bigquery.Dataset(dataset_ref)
dataset.location = "EU"

try:
    client.create_dataset(dataset)
    print(f"Dataset créé : {dataset_id}")
except Exception as e:
    print(f"Dataset existe déjà ou erreur : {e}")

for filename in os.listdir(csv_folder):
    if filename.endswith(".csv"):
        csv_file = os.path.join(csv_folder, filename)
        try:
            df = pd.read_csv(csv_file, on_bad_lines='skip')
        except Exception as e:
            print(f"Erreur lecture CSV {csv_file} : {e}")
            continue

        table_name = os.path.splitext(filename)[0]
        table_ref = dataset_ref.table(table_name)

        try:
            job = client.load_table_from_dataframe(df, table_ref)
            job.result()
            print(f"Fichier chargé : {csv_file} → Table BigQuery : {dataset_id}.{table_name}")
        except Exception as e:
            print(f"Erreur chargement BigQuery pour {csv_file} : {e}")
