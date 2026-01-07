"""Transform - OpenWeatherMap Air Quality Data"""
from datetime import datetime, timezone

def transform_openaq_data(raw_data):
    """Transforme les données brutes OpenWeatherMap"""
    transformed = []
    
    for item in raw_data:
        measurement = item['measurement']
        ingestion_time = datetime.now(timezone.utc).isoformat()
        
        dt = measurement.get('dt')
        measurement_timestamp = datetime.fromtimestamp(dt, tz=timezone.utc).isoformat() if dt else ingestion_time
        
        components = measurement.get('components', {})
        
        row = {
            'city': item['city'],
            'country': item['country'],
            'latitude': item['latitude'],
            'longitude': item['longitude'],
            'ingestion_time': ingestion_time,
            'measurement_timestamp': measurement_timestamp,
            'aqi': measurement.get('main', {}).get('aqi'),
            'co': components.get('co'),
            'no': components.get('no'),
            'no2': components.get('no2'),
            'o3': components.get('o3'),
            'so2': components.get('so2'),
            'pm2_5': components.get('pm2_5'),
            'pm10': components.get('pm10'),
            'nh3': components.get('nh3'),
            'data_source': 'OpenWeatherMap'
        }
        
        transformed.append(row)
    
    return transformed
