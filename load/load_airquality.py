"""Load - Air Quality Data to BigQuery"""
from google.cloud import bigquery

def load_airquality_to_bigquery(df, table_id="projet-fil-orange-477313.air_quality_europe_monthly_avg.donne_open_meteo_monde_qualité_aire"):
    """Charge dans BigQuery"""
    client = bigquery.Client(location="EU")
    
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    
    return len(df)
