import os
import json
import requests
import time
from datetime import datetime, timezone, timedelta
from flask import Flask, request, jsonify
from google.cloud import bigquery

app = Flask(__name__)

# Configuration
PROJECT_ID = os.environ.get('PROJECT_ID')
DATASET_NAME = os.environ.get('DATASET_NAME', 'airquality_full')
TABLE_NAME = os.environ.get('TABLE_NAME', 'measurements_complette')
API_KEY = os.environ.get('OPENWEATHER_API_KEY')

# Client BigQuery
bq_client = bigquery.Client(project=PROJECT_ID)
table_id = f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}"

def ensure_table_exists():
    """Crée la table si elle n'existe pas"""
    try:
        bq_client.get_table(table_id)
        print(f"✅ Table {table_id} existe déjà")
    except Exception as e:
        print(f"⚠️ Table n'existe pas, création en cours...")
        
        schema = [
            bigquery.SchemaField("city", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("country", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("latitude", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("longitude", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("ingestion_time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("measurement_timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("aqi", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("co", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("no", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("no2", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("o3", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("so2", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("pm2_5", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("pm10", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("nh3", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("data_source", "STRING", mode="NULLABLE"),
        ]
        
        table = bigquery.Table(table_id, schema=schema)
        table = bq_client.create_table(table)
        print(f"✅ Table {table_id} créée avec succès!")

# Créer la table au démarrage
ensure_table_exists()

# 30 villes France + 20 villes Europe
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

def get_air_quality_data(city_name, country, lat, lon):
    """Récupère les données de qualité de l'air avec OpenWeatherMap API"""
    url = "http://api.openweathermap.org/data/2.5/air_pollution"
    
    params = {
        "lat": lat,
        "lon": lon,
        "appid": API_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data.get('list') and len(data['list']) > 0:
            return process_measurement(city_name, country, lat, lon, data['list'][0])
        return None
        
    except Exception as e:
        print(f"Erreur pour {city_name}: {str(e)}")
        return None

def get_historical_data(city_name, country, lat, lon):
    """Récupère les données historiques sur 2 ans"""
    url = "http://api.openweathermap.org/data/2.5/air_pollution/history"
    
    # Calculer les timestamps pour les 2 dernières années
    now = datetime.now(timezone.utc)
    two_years_ago = int((now - timedelta(days=730)).timestamp())
    now_timestamp = int(now.timestamp())
    
    params = {
        "lat": lat,
        "lon": lon,
        "start": two_years_ago,
        "end": now_timestamp,
        "appid": API_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        rows = []
        if data.get('list'):
            # Limiter à 100 mesures pour éviter trop de données
            for measurement in data['list'][:100]:
                row = process_measurement(city_name, country, lat, lon, measurement)
                if row:
                    rows.append(row)
        
        return rows
        
    except Exception as e:
        print(f"Erreur historique pour {city_name}: {str(e)}")
        return []

def process_measurement(city_name, country, lat, lon, measurement):
    """Traite une mesure et la formate pour BigQuery"""
    ingestion_time = datetime.now(timezone.utc).isoformat()
    
    # Timestamp de la mesure
    dt = measurement.get('dt')
    measurement_timestamp = datetime.fromtimestamp(dt, tz=timezone.utc).isoformat() if dt else ingestion_time
    
    # AQI
    aqi = measurement.get('main', {}).get('aqi')
    
    # Composants (tous en µg/m³)
    components = measurement.get('components', {})
    
    row = {
        'city': city_name,
        'country': country,
        'latitude': lat,
        'longitude': lon,
        'ingestion_time': ingestion_time,
        'measurement_timestamp': measurement_timestamp,
        'aqi': aqi,
        'co': components.get('co'),           # Carbon monoxide
        'no': components.get('no'),           # Nitrogen monoxide
        'no2': components.get('no2'),         # Nitrogen dioxide
        'o3': components.get('o3'),           # Ozone
        'so2': components.get('so2'),         # Sulphur dioxide
        'pm2_5': components.get('pm2_5'),     # PM2.5
        'pm10': components.get('pm10'),       # PM10
        'nh3': components.get('nh3'),         # Ammonia
        'data_source': 'OpenWeatherMap'
    }
    
    return row

def insert_to_bigquery(rows):
    """Insère les données dans BigQuery"""
    if not rows:
        return 0
    
    # S'assurer que rows est une liste
    if not isinstance(rows, list):
        rows = [rows]
    
    errors = bq_client.insert_rows_json(table_id, rows)
    
    if errors:
        print(f"Erreurs d'insertion: {errors}")
        return 0
    
    return len(rows)

@app.route('/', methods=['GET', 'POST'])
def collect_data():
    """Endpoint principal pour collecter les données"""
    try:
        total_rows = 0
        results = []
        
        # Vérifier si on veut l'historique
        collect_history = request.args.get('history', 'false').lower() == 'true'
        
        print(f"🌍 Collecte des données pour {len(CITIES)} villes (30 FR + 20 EU)...")
        if collect_history:
            print(f"📜 Mode historique activé : 2 dernières années")
        print(f"🔑 API Key: {API_KEY[:10]}..." if API_KEY else "⚠️ Pas de clé API!")
        
        for city in CITIES:
            print(f"📍 Traitement de {city['name']} ({city['country']})...")
            
            if collect_history:
                # Récupérer l'historique
                rows = get_historical_data(
                    city['name'], 
                    city['country'], 
                    city['lat'], 
                    city['lon']
                )
                
                if rows:
                    inserted = insert_to_bigquery(rows)
                    total_rows += inserted
                    print(f"✅ {city['name']}: {inserted} mesures historiques insérées")
                    results.append({
                        'city': city['name'],
                        'country': city['country'],
                        'rows_inserted': inserted,
                        'type': 'historical'
                    })
                else:
                    print(f"⚠️ {city['name']}: Aucune donnée historique disponible")
                    results.append({
                        'city': city['name'],
                        'country': city['country'],
                        'rows_inserted': 0,
                        'type': 'historical'
                    })
            else:
                # Récupérer les données actuelles
                row = get_air_quality_data(
                    city['name'], 
                    city['country'], 
                    city['lat'], 
                    city['lon']
                )
                
                if row:
                    inserted = insert_to_bigquery(row)
                    total_rows += inserted
                    
                    print(f"✅ {city['name']}: 1 mesure insérée (AQI: {row.get('aqi', 'N/A')}, PM2.5: {row.get('pm2_5', 'N/A')} µg/m³)")
                    results.append({
                        'city': city['name'],
                        'country': city['country'],
                        'rows_inserted': inserted,
                        'aqi': row.get('aqi'),
                        'pm2_5': row.get('pm2_5'),
                        'type': 'current'
                    })
                else:
                    print(f"⚠️ {city['name']}: Aucune donnée disponible")
                    results.append({
                        'city': city['name'],
                        'country': city['country'],
                        'rows_inserted': 0,
                        'type': 'current'
                    })
            
            time.sleep(0.2)  # Petit délai pour éviter le rate limiting
        
        return jsonify({
            'status': 'success',
            'total_rows_inserted': total_rows,
            'cities_processed': len(CITIES),
            'french_cities': 30,
            'european_cities': 20,
            'collection_type': 'historical' if collect_history else 'current',
            'details': results,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }), 200
        
    except Exception as e:
        print(f"❌ Erreur: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy'}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
