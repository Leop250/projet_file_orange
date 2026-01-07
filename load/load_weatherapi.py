"""Load - WeatherAPI.com Data to BigQuery"""
from google.cloud import bigquery
import pandas as pd

def load_weatherapi_to_bigquery(data, project_id="projet-fil-orange-477313", table_id="weather_data.weatherapicom"):
    """Charge dans BigQuery"""
    df = pd.DataFrame(data)
    
    client = bigquery.Client(project=project_id)
    full_table_id = f"{project_id}.{table_id}"
    
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
    job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    job.result()
    
    return len(df)
