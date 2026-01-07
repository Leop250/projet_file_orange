"""Transform - OpenWeatherMap Weather Data"""
from datetime import datetime

def transform_openweathermap_data(raw_data):
    """Transforme les données météo OpenWeatherMap"""
    transformed = []
    
    for item in raw_data:
        data = item['data']
        main = data.get("main", {})
        weather_list = data.get("weather", [{}])
        
        row = {
            "city": item['city'],
            "temperature": main.get("temp"),
            "humidity": main.get("humidity"),
            "weather": weather_list[0].get("description"),
            "timestamp": datetime.utcnow()
        }
        
        transformed.append(row)
    
    return transformed
