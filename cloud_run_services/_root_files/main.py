import os
import requests
from datetime import datetime, timezone
from flask import Flask, jsonify
from google.cloud import bigquery

PROJECT_ID = os.environ.get("GCP_PROJECT", "projet-fil-orange-477313")
DATASET_ID = "air_quality"
TABLE_ID = "measurements"

LATITUDE = 48.8566
LONGITUDE = 2.3522

AIR_QUALITY_API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

app = Flask(__name__)
bq_client = bigquery.Client(project=PROJECT_ID)

@app.route("/", methods=["GET"])
def health():
    return "OK", 200

@app.route("/run", methods=["POST"])
def run_job():
    try:
        params = {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "hourly": "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone",
            "timezone": "Europe/Paris",
        }

        r = requests.get(AIR_QUALITY_API_URL, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        i = -1
        record = {
            "timestamp": data["hourly"]["time"][i],
            "pm10": data["hourly"]["pm10"][i],
            "pm2_5": data["hourly"]["pm2_5"][i],
            "carbon_monoxide": data["hourly"]["carbon_monoxide"][i],
            "nitrogen_dioxide": data["hourly"]["nitrogen_dioxide"][i],
            "ozone": data["hourly"]["ozone"][i],
            "ingestion_time": datetime.now(timezone.utc).isoformat(),
            "city": "Paris",
        }

        table = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        errors = bq_client.insert_rows_json(table, [record])

        if errors:
            return jsonify({"status": "error", "errors": errors}), 500

        return jsonify({"status": "success", "data": record}), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
