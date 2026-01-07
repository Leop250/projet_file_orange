"""Extract - WeatherAPI.com Data"""
import requests
from datetime import datetime

CITIES = ["Paris", "Berlin", "Bruxelles"]

def extract_weather_weatherapi(api_key):
    """Extrait depuis WeatherAPI.com"""
    results = []
    
    for city in CITIES:
        try:
            url = f"http://api.weatherapi.com/v1/current.json?key={api_key}&q={city}"
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            data = r.json()
            results.append({'city': city, 'data': data})
        except Exception as e:
            print(f"Erreur {city}: {str(e)}")
    
    return results
