import os
import requests
import pandas as pd


API_KEY = os.getenv("OPENAQ_API_KEY")
HEADERS = {"X-API-Key": API_KEY}


europe_countries = [
    "FR", "DE", "IT", "ES", "PL", "RO", "NL", "BE", "GR", "CZ",
    "PT", "SE", "HU", "AT", "BG", "DK", "FI", "SK", "IE", "HR",
    "LT", "SI", "LV", "EE", "CY", "LU", "MT"
]

base_url = "https://api.openaq.org/v3"

def get_country_locations(country_code):

    url = f"{base_url}/locations"
    params = {
        "country": country_code,
        "parameter": "pm25",
        "limit": 100
    }
    resp = requests.get(url, headers=HEADERS, params=params)
    if resp.status_code != 200:
        print(f" Erreur pour {country_code}: {resp.json()}")
        return []
    data = resp.json()
    return data.get("results", [])

def get_latest_pm25(location_id):
    """Récupère la dernière valeur PM2.5 pour une location"""
    url = f"{base_url}/locations/{location_id}/latest"
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code != 200:
        return None
    data = resp.json()
    results = data.get("results", [])
    for r in results:
        for m in r.get("measurements", []):
            if m["parameter"] == "pm25":
                return m["value"]
    return None

# Stockage des moyennes par pays
country_pm25 = {}

for country in europe_countries:
    print(f"Récupération des données pour {country}...")
    locations = get_country_locations(country)
    if not locations:
        continue
    values = []
    for loc in locations:
        val = get_latest_pm25(loc["id"])
        if val is not None:
            values.append(val)
    if values:
        country_pm25[country] = sum(values) / len(values)


top10 = sorted(country_pm25.items(), key=lambda x: x[1], reverse=True)[:10]

df = pd.DataFrame(top10, columns=["Country", "Avg_PM25"])
print("\nTop 10 des pays européens les plus pollués (PM2.5) :")
print(df)
