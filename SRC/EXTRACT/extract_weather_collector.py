"""Extract - Weather Collector (OpenWeatherMap)"""
import requests

CITIES = [

    {"name": "Paris", "country": "FR", "lat": 48.8566, "lon": 2.3522},
    {"name": "Marseille", "country": "FR", "lat": 43.2965, "lon": 5.3698},
    {"name": "Lyon", "country": "FR", "lat": 45.7640, "lon": 4.8357},
    {"name": "Toulouse", "country": "FR", "lat": 43.6047, "lon": 1.4442},
    {"name": "Nice", "country": "FR", "lat": 43.7102, "lon": 7.2620},
    {"name": "Nantes", "country": "FR", "lat": 47.2184, "lon": -1.5536},
    {"name": "Montpellier", "country": "FR", "lat": 43.6108, "lon": 3.8767},
    {"name": "Strasbourg", "country": "FR", "lat": 48.5734, "lon": 7.7521},
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
    {"name": "Villeurbanne", "country": "FR", "lat": 45.7667, "lon": 4.8833},
    {"name": "Clermont-Ferrand", "country": "FR", "lat": 45.7772, "lon": 3.0870},
    {"name": "Aix-en-Provence", "country": "FR", "lat": 43.5297, "lon": 5.4474},
    {"name": "Brest", "country": "FR", "lat": 48.3904, "lon": -4.4861},
    {"name": "Limoges", "country": "FR", "lat": 45.8336, "lon": 1.2611},
    {"name": "Tours", "country": "FR", "lat": 47.3941, "lon": 0.6848},
    {"name": "Amiens", "country": "FR", "lat": 49.8941, "lon": 2.2957},
    {"name": "Perpignan", "country": "FR", "lat": 42.6887, "lon": 2.8948},
    {"name": "Metz", "country": "FR", "lat": 49.1193, "lon": 6.1757},
    {"name": "Besançon", "country": "FR", "lat": 47.2378, "lon": 6.0241},
    {"name": "Orléans", "country": "FR", "lat": 47.9029, "lon": 1.9093},


    {"name": "London", "country": "GB", "lat": 51.5074, "lon": -0.1278},
    {"name": "Berlin", "country": "DE", "lat": 52.5200, "lon": 13.4050},
    {"name": "Madrid", "country": "ES", "lat": 40.4168, "lon": -3.7038},
    {"name": "Rome", "country": "IT", "lat": 41.9028, "lon": 12.4964},
    {"name": "Milan", "country": "IT", "lat": 45.4642, "lon": 9.1900},
    {"name": "Barcelona", "country": "ES", "lat": 41.3851, "lon": 2.1734},
    {"name": "Amsterdam", "country": "NL", "lat": 52.3676, "lon": 4.9041},
    {"name": "Brussels", "country": "BE", "lat": 50.8503, "lon": 4.3517},
    {"name": "Vienna", "country": "AT", "lat": 48.2082, "lon": 16.3738},
    {"name": "Prague", "country": "CZ", "lat": 50.0755, "lon": 14.4378},
    {"name": "Warsaw", "country": "PL", "lat": 52.2297, "lon": 21.0122},
    {"name": "Budapest", "country": "HU", "lat": 47.4979, "lon": 19.0402},
    {"name": "Copenhagen", "country": "DK", "lat": 55.6761, "lon": 12.5683},
    {"name": "Stockholm", "country": "SE", "lat": 59.3293, "lon": 18.0686},
    {"name": "Oslo", "country": "NO", "lat": 59.9139, "lon": 10.7522},
    {"name": "Helsinki", "country": "FI", "lat": 60.1699, "lon": 24.9384},
    {"name": "Lisbon", "country": "PT", "lat": 38.7223, "lon": -9.1393},
    {"name": "Dublin", "country": "IE", "lat": 53.3498, "lon": -6.2603},
    {"name": "Athens", "country": "GR", "lat": 37.9838, "lon": 23.7275},
    {"name": "Zurich", "country": "CH", "lat": 47.3769, "lon": 8.5417},
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
