"""Extract - OpenWeatherMap Weather Data"""
import requests

CITIES = ["Paris", "Marseille", "Lyon", "Toulouse", "Nice", "Nantes", "Strasbourg", "Montpellier", "Bordeaux", "Lille"]

def extract_weather_openweathermap(api_key):
    """Extrait les données météo OpenWeatherMap"""
    results = []
    
    for city in CITIES:
        try:
            url = f"http://api.openweathermap.org/data/2.5/weather?q={city},FR&appid={api_key}&units=metric"
            resp = requests.get(url, timeout=10)
            
            if resp.status_code == 200:
                data = resp.json()
                results.append({'city': city, 'data': data})
            else:
                print(f"Erreur API {city}: {resp.status_code}")
                
        except Exception as e:
            print(f"Erreur {city}: {str(e)}")
            
    return results
