from flask import Flask, jsonify
import requests
import pandas as pd
import time
from datetime import datetime
from google.cloud import bigquery
import os

app = Flask(__name__)

@app.route("/", methods=["GET"])
def home():
    return jsonify({"status": "ok", "service": "Air Quality API"}), 200

@app.route("/fetch", methods=["POST", "GET"])
def fetch_air_quality():
    try:
        countries = {
            "France": (48.8566, 2.3522),
            "Germany": (52.5200, 13.4050),
            "Belgium": (50.8503, 4.3517)
        }

        variables = ["pm2_5", "pm10", "nitrogen_dioxide", "ozone"]
        current_year = datetime.now().year
        today = datetime.now().strftime("%Y-%m-%d")
        years = [2024, current_year]

        all_data = []

        for country, (lat, lon) in countries.items():
            for year in years:
                start_date = f"{year}-01-01"
                end_date = today if year == current_year else f"{year}-12-31"

                url = f"https://air-quality-api.open-meteo.com/v1/air-quality?latitude={lat}&longitude={lon}&start_date={start_date}&end_date={end_date}&hourly={','.join(variables)}"

                r = requests.get(url, timeout=30)
                r.raise_for_status()
                data = r.json()

                if "hourly" in data and data["hourly"]:
                    df = pd.DataFrame(data["hourly"])
                    df["time"] = pd.to_datetime(df["time"])
                    df["country"] = country
                    df["year"] = year
                    all_data.append(df)
                time.sleep(0.5)

        if not all_data:
            return jsonify({"status": "error", "message": "Pas de données"}), 500

        df_all = pd.concat(all_data, ignore_index=True)
        df_all["month"] = df_all["time"].dt.to_period("M")
        monthly_avg = df_all.groupby(["country", "year", "month"])[variables].mean().reset_index()
        monthly_avg["month"] = monthly_avg["month"].astype(str)

        client = bigquery.Client(location="EU")
        table_id = "projet-fil-orange-477313.air_quality_europe_monthly_avg.donne_open_meteo_monde_qualité_aire"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(monthly_avg, table_id, job_config=job_config)
        job.result()

        return jsonify({"status": "success", "rows": len(monthly_avg)}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
