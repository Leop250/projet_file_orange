"""Load - OpenWeatherMap Weather Data to BigQuery"""
from google.cloud import bigquery

def load_openweathermap_to_bigquery(data, project_id="projet-fil-orange-477313", dataset_id="openweathermap", table_id="WeatherData"):
    """Charge dans BigQuery"""
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    errors = client.insert_rows_json(table_ref, data)
    
    if errors:
        print(f"Erreurs: {errors}")
        return 0
    
    return len(data)
