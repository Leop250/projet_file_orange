"""Transform - WeatherAPI.com Data"""
from datetime import datetime

def transform_weatherapi_data(raw_data):
    """Transforme les données WeatherAPI.com"""
    transformed = []
    
    for item in raw_data:
        data = item['data']
        row = {
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
        transformed.append(row)
    
    return transformed
