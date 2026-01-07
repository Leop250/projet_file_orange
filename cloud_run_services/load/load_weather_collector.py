"""Load - Weather Collector Data to BigQuery"""
from google.cloud import bigquery

def load_weather_collector_to_bigquery(data, project_id, dataset_id="weather_data", table_id="weather_records"):
    """Charge dans BigQuery"""
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    errors = client.insert_rows_json(table_ref, data)
    
    if errors:
        print(f"Erreurs: {errors}")
        return 0
    
    return len(data)
