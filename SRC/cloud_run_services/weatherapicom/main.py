from flask import Flask, jsonify
import requests
import pandas as pd
import os
from datetime import datetime
from google.cloud import bigquery

app = Flask(__name__)

API_KEY = os.environ.get("WEATHER_API_KEY", "4441877a482e4d309b2143538260101")
CITIES = ["Paris", "Berlin", "Bruxelles"]

@app.route("/", methods=["GET"])
def home():
    return jsonify({"status": "ok", "service": "Weather API Fetcher"}), 200

@app.route("/fetch_weather", methods=["GET"])
def fetch_weather():
    try:
        all_data = []
        
        for city in CITIES:
            url = f"http://api.weatherapi.com/v1/current.json?key={API_KEY}&q={city}"
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            data = r.json()
            
            city_data = {
                "city": data["location"]["name"],
                "region": data["location"]["region"],
                "country": data["location"]["country"],
                "lat": data["location"]["lat"],
                "lon": data["location"]["lon"],
                "localtime": data["location"]["localtime"],
                "temp_c": data["current"]["temp_c"],
                "condition": data["current"]["condition"]["text"],
                "wind_kph": data["current"]["wind_kph"],
                "humidity": data["current"]["humidity"],
                "cloud": data["current"]["cloud"],
                "timestamp": datetime.utcnow()
            }
            all_data.append(city_data)
        
        df = pd.DataFrame(all_data)
        
        bigquery_status = "not_attempted"
        try:
            # CORRECTION : Utiliser europe-west1 au lieu de EU
            client = bigquery.Client(project="projet-fil-orange-477313")
            table_id = "projet-fil-orange-477313.weather_data.weatherapicom"
            
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                autodetect=True
            )
            
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()
            bigquery_status = "success"
            
        except Exception as bq_error:
            print(f"❌ Erreur BigQuery: {bq_error}")
            bigquery_status = f"error: {str(bq_error)}"
        
        return jsonify({
            "status": "success",
            "rows": len(df),
            "bigquery_status": bigquery_status,
            "data": df.to_dict(orient="records")
        }), 200
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
