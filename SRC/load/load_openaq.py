"""Load - OpenWeatherMap Air Quality Data to BigQuery"""
from google.cloud import bigquery

def load_openaq_to_bigquery(data, project_id, dataset_name="airquality_full", table_name="measurements_complette"):
    """Charge les données dans BigQuery"""
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset_name}.{table_name}"
    
    errors = client.insert_rows_json(table_id, data)
    
    if errors:
        print(f"Erreurs d'insertion: {errors}")
        return 0
    
    return len(data)
