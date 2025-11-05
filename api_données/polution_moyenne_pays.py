import requests
import pandas as pd
from time import sleep

API_KEY = "1380c61b7f18de2c4640671379dbb49d82208279b19a55cf72cd6a2907e3ffec"
headers = {"X-API-Key": API_KEY}

def get_pm25_measurements(country_code, limit=100):
    url = "https://api.openaq.org/v3/measurements"  # <-- attention à bien mettre /measurements
    params = {
        "country": country_code,
        "parameter": "pm25",
        "limit": limit,
        "order_by": "datetime",
        "sort": "desc"
    }
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code != 200:
        raise Exception(f"Erreur API {resp.status_code}: {resp.text}")
    data = resp.json().get("results", [])
    if not data:
        raise Exception(f"Aucune mesure PM2.5 trouvée pour {country_code}")
    df = pd.json_normalize(data)
    return df

countries = ["FR", "DE", "IT", "ES", "US"]
all_data = []

for country in countries:
    try:
        df_country = get_pm25_measurements(country, limit=100)
        print(f"{country} : {len(df_country)} mesures récupérées")
        df_country["country"] = country
        all_data.append(df_country)
    except Exception as e:
        print(f"⚠️ {e}")
    sleep(1)

if all_data:
    df_all = pd.concat(all_data, ignore_index=True)
    df_all.to_csv("pm25_par_pays.csv", index=False)
    print("✅ CSV prêt pour BigQuery : pm25_par_pays.csv")
