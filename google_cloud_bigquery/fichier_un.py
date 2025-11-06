from google.cloud import bigquery
import pandas as pd

# Chemin vers ton fichier CSV
csv_file = r"/projet_file_orange/fichier_csv/air_quality_europe_monthly_avg.csv"

# Nom du projet et dataset BigQuery
project_id = "projet-fil-orange-477313"
dataset_id = "air_quality_europe_monthly_avg"
table_id = "air_quality_monthly_avg"


df = pd.read_csv(csv_file)

client = bigquery.Client.from_service_account_json("keyfile.json", project=project_id)

# Définir la référence à la table
table_ref = client.dataset(dataset_id).table(table_id)

# Charger le dataframe dans BigQuery
job = client.load_table_from_dataframe(df, table_ref)

# Attendre que le job se termine
job.result()

print(f"Le fichier CSV a été chargé dans BigQuery : {dataset_id}.{table_id}")
