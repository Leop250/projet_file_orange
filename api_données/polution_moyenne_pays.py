import requests
import pandas as pd
from time import sleep

API_KEY = "1380c61b7f18de2c4640671379dbb49d82208279b19a55cf72cd6a2907e3ffec"
headers = {"X-API-Key": API_KEY}

def get_locations_for_param(country_code, parameter_id=2, limit=500):
    """Récupère les locations dans un pays pour un paramètre donné."""
    url = "https://api.openaq.org/v3/locations"
    params = {
        "country_id": country_code,
        "parameters_id": parameter_id,
        "limit": limit
    }
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    return resp.json().get("results", [])

def get_measurements_for_sensor(sensor_id, parameter="pm25", limit=100):
    """Récupère les mesures d’un capteur donné."""
    url = f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements"
    params = {
        "parameter": parameter,
        "limit": limit
    }
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    return resp.json().get("results", [])

def compute_country_mean(country_code):
    locations = get_locations_for_param(country_code)
    all_values = []

    for loc in locations:
        for sensor in loc.get("sensors", []):
            if sensor.get("parameter") == "pm25":
                try:
                    meas = get_measurements_for_sensor(sensor["id"], limit=50)
                    values = [m["value"] for m in meas if m["parameter"] == "pm25"]
                    all_values.extend(values)
                except Exception as e:
                    print(f"⚠️ Problème capteur {sensor['id']}: {e}")
        sleep(0.5)  # pause

    if not all_values:
        raise RuntimeError(f"Aucune mesure PM2.5 récupérée pour le pays {country_code}")

    return sum(all_values) / len(all_values), len(all_values)

countries = ["FR", "DE", "IT"]
results = []

for country in countries:
    try:
        mean_val, count = compute_country_mean(country)
        print(f"{country} : Moyenne PM2.5 = {mean_val:.2f} µg/m³ sur {count} mesures")
        results.append({"country": country, "mean_pm25": mean_val, "count": count})
    except Exception as e:
        print(f"⚠️ {country} => {e}")

df = pd.DataFrame(results)
df.to_csv("pm25_mean_by_country.csv", index=False)
print("✅ CSV prêt")
