"""Transform - Weather Collector Data"""
from datetime import datetime

def transform_weather_collector_data(raw_data):
    """Transforme les données météo complètes"""
    transformed = []
    
    for item in raw_data:
        city_info = item['city_info']
        data = item['data']
        
        row = {
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
        
        transformed.append(row)
    
    return transformed
