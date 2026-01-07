"""Extract - Weather Collector (OpenWeatherMap)"""
import requests

CITIES = [
    # 30 villes France + 20 Europe (même liste que openaq)
    {"name": "Paris", "country": "FR", "lat": 48.8566, "lon": 2.3522},
    {"name": "Marseille", "country": "FR", "lat": 43.2965, "lon": 5.3698},
    # ... (copier la même liste complète)
]

def extract_weather_collector(api_key):
    """Extrait les données météo complètes"""
    results = []
    
    for city in CITIES:
        try:
            url = "https://api.openweathermap.org/data/2.5/weather"
            params = {"lat": city["lat"], "lon": city["lon"], "appid": api_key, "units": "metric"}
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            results.append({'city_info': city, 'data': data})
        except Exception as e:
            print(f"Erreur {city['name']}: {str(e)}")
    
    return results
