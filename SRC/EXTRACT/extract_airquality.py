"""Extract - Air Quality Open-Meteo"""
import requests
import pandas as pd
import time
from datetime import datetime

def extract_air_quality_openmeteo():
    """Extrait depuis Open-Meteo API"""
    countries = {
        "France": (48.8566, 2.3522),
        "Germany": (52.5200, 13.4050),
        "Belgium": (50.8503, 4.3517)
    }
    variables = ["pm2_5", "pm10", "nitrogen_dioxide", "ozone"]
    current_year = datetime.now().year
    today = datetime.now().strftime("%Y-%m-%d")
    years = [2024, current_year]
    
    all_data = []
    
    for country, (lat, lon) in countries.items():
        for year in years:
            start_date = f"{year}-01-01"
            end_date = today if year == current_year else f"{year}-12-31"
            
            url = f"https://air-quality-api.open-meteo.com/v1/air-quality?latitude={lat}&longitude={lon}&start_date={start_date}&end_date={end_date}&hourly={','.join(variables)}"
            
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            data = r.json()
            
            if "hourly" in data and data["hourly"]:
                df = pd.DataFrame(data["hourly"])
                df["time"] = pd.to_datetime(df["time"])
                df["country"] = country
                df["year"] = year
                all_data.append(df)
                
            time.sleep(0.5)
    
    if not all_data:
        raise ValueError("Aucune donnée extraite")
    
    return pd.concat(all_data, ignore_index=True)
