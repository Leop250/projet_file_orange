from flask import Flask, jsonify
import requests
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from datetime import datetime
import os

app = Flask(__name__)

# Liste des principales villes françaises
CITIES = [
    "Paris", "Marseille", "Lyon", "Toulouse", "Nice",
    "Nantes", "Strasbourg", "Montpellier", "Bordeaux", "Lille"
]

# Paramètres BigQuery
PROJECT_ID = "projet-fil-orange-477313"
DATASET_ID = "openweathermap"
TABLE_ID = "WeatherData"

def create_dataset_if_not_exists(client):
    try:
        client.get_dataset(f"{PROJECT_ID}.{DATASET_ID}")
        print(f"Dataset {DATASET_ID} existe déjà.")
    except NotFound:
        dataset = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
        dataset.location = "EU"
        client.create_dataset(dataset)
        print(f"Dataset {DATASET_ID} créé.")

def create_table_if_not_exists(client):
    try:
        client.get_table(f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
        print(f"Table {TABLE_ID} existe déjà.")
    except NotFound:
        schema = [
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("temperature", "FLOAT"),
            bigquery.SchemaField("humidity", "INTEGER"),
            bigquery.SchemaField("weather", "STRING"),
            bigquery.SchemaField("timestamp", "TIMESTAMP")
        ]
        table = bigquery.Table(f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", schema=schema)
        client.create_table(table)
        print(f"Table {TABLE_ID} créée.")

@app.route("/", methods=["GET"])
def openweathermap():
    api_key = os.environ.get("OPENWEATHERMAP_API_KEY")
    if not api_key:
        print("Clé API OpenWeatherMap manquante !")
        return "Clé API OpenWeatherMap manquante", 500

    client = bigquery.Client(project=PROJECT_ID)
    create_dataset_if_not_exists(client)
    create_table_if_not_exists(client)

    rows_to_insert = []

    for city in CITIES:
        try:
            url = f"http://api.openweathermap.org/data/2.5/weather?q={city},FR&appid={api_key}&units=metric"
            resp = requests.get(url, timeout=10)
            print(f"[INFO] {city} - Status API: {resp.status_code}")
            data = resp.json()

            if resp.status_code != 200:
                print(f"[WARN] API Error {city}: {data}")
                continue

            main = data.get("main", {})
            weather_list = data.get("weather", [{}])
            rows_to_insert.append({
                "city": city,
                "temperature": main.get("temp"),
                "humidity": main.get("humidity"),
                "weather": weather_list[0].get("description"),
                "timestamp": datetime.utcnow()
            })
        except Exception as e:
            print(f"[ERROR] {city} - {e}")

    # Insertion BigQuery
    try:
        if rows_to_insert:
            errors = client.insert_rows_json(f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", rows_to_insert)
            if errors:
                print(f"[ERROR] BigQuery insert errors: {errors}")
                return jsonify({"status": "error", "details": errors}), 500
    except Exception as e:
        print(f"[ERROR] BigQuery exception: {e}")
        return f"Erreur BigQuery: {e}", 500

    print(f"[SUCCESS] Données météo envoyées pour {len(rows_to_insert)} villes !")
    return jsonify({"status": "success", "cities_inserted": len(rows_to_insert)}), 200
