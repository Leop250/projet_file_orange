from flask import Flask, jsonify
import requests
from google.cloud import bigquery
from datetime import datetime
import os
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# Configuration
API_KEY = os.environ.get('WEATHER_API_KEY')
PROJECT_ID = os.environ.get('GCP_PROJECT')
DATASET_ID = "weather_data"
TABLE_ID = "weather_records"

# 30 plus grandes villes de France + grandes villes d'Europe
CITIES = [
    # 30 plus grandes villes de France
    {"name": "Paris", "country": "FR", "lat": 48.8566, "lon": 2.3522},
    {"name": "Marseille", "country": "FR", "lat": 43.2965, "lon": 5.3698},
    {"name": "Lyon", "country": "FR", "lat": 45.7640, "lon": 4.8357},
    {"name": "Toulouse", "country": "FR", "lat": 43.6047, "lon": 1.4442},
    {"name": "Nice", "country": "FR", "lat": 43.7102, "lon": 7.2620},
    {"name": "Nantes", "country": "FR", "lat": 47.2184, "lon": -1.5536},
    {"name": "Strasbourg", "country": "FR", "lat": 48.5734, "lon": 7.7521},
    {"name": "Montpellier", "country": "FR", "lat": 43.6108, "lon": 3.8767},
    {"name": "Bordeaux", "country": "FR", "lat": 44.8378, "lon": -0.5792},
    {"name": "Lille", "country": "FR", "lat": 50.6292, "lon": 3.0573},
    {"name": "Rennes", "country": "FR", "lat": 48.1173, "lon": -1.6778},
    {"name": "Reims", "country": "FR", "lat": 49.2583, "lon": 4.0317},
    {"name": "Le Havre", "country": "FR", "lat": 49.4944, "lon": 0.1079},
    {"name": "Saint-Étienne", "country": "FR", "lat": 45.4397, "lon": 4.3872},
    {"name": "Toulon", "country": "FR", "lat": 43.1242, "lon": 5.9280},
    {"name": "Grenoble", "country": "FR", "lat": 45.1885, "lon": 5.7245},
    {"name": "Dijon", "country": "FR", "lat": 47.3220, "lon": 5.0415},
    {"name": "Angers", "country": "FR", "lat": 47.4784, "lon": -0.5632},
    {"name": "Nîmes", "country": "FR", "lat": 43.8367, "lon": 4.3601},
    {"name": "Villeurbanne", "country": "FR", "lat": 45.7667, "lon": 4.8800},
    {"name": "Le Mans", "country": "FR", "lat": 48.0077, "lon": 0.1984},
    {"name": "Aix-en-Provence", "country": "FR", "lat": 43.5297, "lon": 5.4474},
    {"name": "Clermont-Ferrand", "country": "FR", "lat": 45.7772, "lon": 3.0870},
    {"name": "Brest", "country": "FR", "lat": 48.3905, "lon": -4.4860},
    {"name": "Limoges", "country": "FR", "lat": 45.8336, "lon": 1.2611},
    {"name": "Tours", "country": "FR", "lat": 47.3941, "lon": 0.6848},
    {"name": "Amiens", "country": "FR", "lat": 49.8942, "lon": 2.2957},
    {"name": "Perpignan", "country": "FR", "lat": 42.6987, "lon": 2.8948},
    {"name": "Metz", "country": "FR", "lat": 49.1193, "lon": 6.1757},
    {"name": "Besançon", "country": "FR", "lat": 47.2380, "lon": 6.0243},
    
    # Grandes villes d'Europe
    {"name": "London", "country": "GB", "lat": 51.5074, "lon": -0.1278},
    {"name": "Berlin", "country": "DE", "lat": 52.5200, "lon": 13.4050},
    {"name": "Madrid", "country": "ES", "lat": 40.4168, "lon": -3.7038},
    {"name": "Rome", "country": "IT", "lat": 41.9028, "lon": 12.4964},
    {"name": "Barcelona", "country": "ES", "lat": 41.3851, "lon": 2.1734},
    {"name": "Vienna", "country": "AT", "lat": 48.2082, "lon": 16.3738},
    {"name": "Hamburg", "country": "DE", "lat": 53.5511, "lon": 9.9937},
    {"name": "Munich", "country": "DE", "lat": 48.1351, "lon": 11.5820},
    {"name": "Milan", "country": "IT", "lat": 45.4642, "lon": 9.1900},
    {"name": "Prague", "country": "CZ", "lat": 50.0755, "lon": 14.4378},
    {"name": "Brussels", "country": "BE", "lat": 50.8503, "lon": 4.3517},
    {"name": "Amsterdam", "country": "NL", "lat": 52.3676, "lon": 4.9041},
    {"name": "Lisbon", "country": "PT", "lat": 38.7223, "lon": -9.1393},
    {"name": "Athens", "country": "GR", "lat": 37.9838, "lon": 23.7275},
    {"name": "Stockholm", "country": "SE", "lat": 59.3293, "lon": 18.0686},
    {"name": "Warsaw", "country": "PL", "lat": 52.2297, "lon": 21.0122},
    {"name": "Budapest", "country": "HU", "lat": 47.4979, "lon": 19.0402},
    {"name": "Copenhagen", "country": "DK", "lat": 55.6761, "lon": 12.5683},
    {"name": "Dublin", "country": "IE", "lat": 53.3498, "lon": -6.2603},
    {"name": "Oslo", "country": "NO", "lat": 59.9139, "lon": 10.7522},
]

def create_bigquery_table():
    """Crée la table BigQuery si elle n'existe pas"""
    client = bigquery.Client(project=PROJECT_ID)
    
    # Créer le dataset
    dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {DATASET_ID} existe déjà")
    except:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "EU"
        client.create_dataset(dataset)
        print(f"Dataset {DATASET_ID} créé")
    
    # Créer la table
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    schema = [
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("city_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("country", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("latitude", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("longitude", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("temperature", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("feels_like", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("temp_min", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("temp_max", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("pressure", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("humidity", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("weather_main", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("weather_description", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("wind_speed", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("wind_deg", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("wind_gust", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("clouds", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("visibility", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("sunrise", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("sunset", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("timezone", "INTEGER", mode="NULLABLE"),
    ]
    
    try:
        client.get_table(table_ref)
        print(f"Table {TABLE_ID} existe déjà")
    except:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"Table {TABLE_ID} créée")

def get_weather_data(city_info):
    """Récupère les données météo pour une ville"""
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "lat": city_info["lat"],
        "lon": city_info["lon"],
        "appid": API_KEY,
        "units": "metric"
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        weather_record = {
            "timestamp": datetime.utcnow().isoformat(),
            "city_name": city_info["name"],
            "country": city_info["country"],
            "latitude": city_info["lat"],
            "longitude": city_info["lon"],
            "temperature": data["main"]["temp"],
            "feels_like": data["main"]["feels_like"],
            "temp_min": data["main"]["temp_min"],
            "temp_max": data["main"]["temp_max"],
            "pressure": data["main"]["pressure"],
            "humidity": data["main"]["humidity"],
            "weather_main": data["weather"][0]["main"],
            "weather_description": data["weather"][0]["description"],
            "wind_speed": data.get("wind", {}).get("speed"),
            "wind_deg": data.get("wind", {}).get("deg"),
            "wind_gust": data.get("wind", {}).get("gust"),
            "clouds": data.get("clouds", {}).get("all"),
            "visibility": data.get("visibility"),
            "sunrise": datetime.utcfromtimestamp(data["sys"]["sunrise"]).isoformat() if "sys" in data and "sunrise" in data["sys"] else None,
            "sunset": datetime.utcfromtimestamp(data["sys"]["sunset"]).isoformat() if "sys" in data and "sunset" in data["sys"] else None,
            "timezone": data.get("timezone"),
        }
        
        return weather_record
    except Exception as e:
        print(f"Erreur pour {city_info['name']}: {str(e)}")
        return None

def insert_to_bigquery(records):
    """Insère les données dans BigQuery"""
    if not records:
        return
    
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    errors = client.insert_rows_json(table_ref, records)
    
    if errors:
        print(f"Erreurs lors de l'insertion: {errors}")
    else:
        print(f"{len(records)} enregistrements insérés avec succès")

@app.route('/collect', methods=['GET', 'POST'])
def collect_weather():
    """Endpoint pour collecter les données météo"""
    try:
        if not API_KEY:
            return jsonify({"status": "error", "message": "WEATHER_API_KEY non définie"}), 500
        
        # Créer la table si nécessaire
        create_bigquery_table()
        
        # Collecter les données
        weather_records = []
        for city in CITIES:
            record = get_weather_data(city)
            if record:
                weather_records.append(record)
        
        # Insérer dans BigQuery
        insert_to_bigquery(weather_records)
        
        return jsonify({
            "status": "success",
            "records_collected": len(weather_records),
            "total_cities": len(CITIES),
            "french_cities": len([c for c in CITIES if c["country"] == "FR"]),
            "european_cities": len([c for c in CITIES if c["country"] != "FR"]),
        }), 200
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/health', methods=['GET'])
def health():
    """Endpoint de santé"""
    return jsonify({"status": "healthy", "total_cities": len(CITIES)}), 200

@app.route('/', methods=['GET'])
def home():
    """Page d'accueil"""
    return jsonify({
        "service": "Weather Data Collector",
        "total_cities": len(CITIES),
        "french_cities": len([c for c in CITIES if c["country"] == "FR"]),
        "european_cities": len([c for c in CITIES if c["country"] != "FR"]),
        "endpoints": {
            "/collect": "Collecter les données météo",
            "/health": "Vérifier la santé du service"
        }
    }), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
