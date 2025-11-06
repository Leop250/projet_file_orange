from google.cloud import bigquery
import pandas as pd
import os

# --------------------------
# CONFIGURATION
# --------------------------
csv_file = r"C:\HETIC\2025-2026\Projet_File_0range\projet_file_orange\fichier_csv\air_quality_europe_monthly_avg.csv"
project_id = "projet-fil-orange-477313"
dataset_id = "air_quality_europe_monthly_avg"
table_id = "air_quality_monthly_avg"
keyfile_path = r"C:\HETIC\2025-2026\Projet_File_0range\projet_file_orange\google_cloud_bigquery\keyfile.json"

# --------------------------
# CHARGER LE CSV
# --------------------------
if not os.path.exists(csv_file):
    raise FileNotFoundError(f"Le fichier CSV n'existe pas : {csv_file}")

df = pd.read_csv(csv_file)

# --------------------------
# INITIALISER LE CLIENT BIGQUERY
# --------------------------
client = bigquery.Client.from_service_account_json(keyfile_path, project=project_id)

# --------------------------
# CREER LE DATASET SI INEXISTANT
# --------------------------
dataset_ref = client.dataset(dataset_id)
dataset = bigquery.Dataset(dataset_ref)
dataset.location = "EU"  # Modifier si nécessaire

try:
    client.create_dataset(dataset)  # Crée le dataset si inexistant
    print(f"Dataset créé : {dataset_id}")
except Exception as e:
    print(f"Dataset existe déjà ou erreur : {e}")

# --------------------------
# CHARGER LE DATAFRAME DANS BIGQUERY
# --------------------------
table_ref = dataset_ref.table(table_id)

job = client.load_table_from_dataframe(df, table_ref)

job.result()  # Attendre la fin du job

print(f"Le fichier CSV a été chargé dans BigQuery : {dataset_id}.{table_id}")
