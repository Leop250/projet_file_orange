import requests
import pandas as pd
import time
from datetime import datetime

# Liste des pays d'Europe avec leurs capitales (latitude, longitude)
countries = {
    "Albania": (41.3275, 19.8189),
    "Andorra": (42.5078, 1.5211),
    "Austria": (48.2082, 16.3738),
    "Belgium": (50.8503, 4.3517),
    "Bosnia and Herzegovina": (43.8563, 18.4131),
    "Bulgaria": (42.6977, 23.3219),
    "Croatia": (45.8150, 15.9819),
    "Cyprus": (35.1856, 33.3823),
    "Czech Republic": (50.0755, 14.4378),
    "Denmark": (55.6761, 12.5683),
    "Estonia": (59.4370, 24.7536),
    "Finland": (60.1695, 24.9354),
    "France": (48.8566, 2.3522),
    "Germany": (52.5200, 13.4050),
    "Greece": (37.9838, 23.7275),
    "Hungary": (47.4979, 19.0402),
    "Ireland": (53.3498, -6.2603),
    "Italy": (41.9028, 12.4964),
    "Latvia": (56.9496, 24.1052),
    "Lithuania": (54.6872, 25.2797),
    "Luxembourg": (49.6117, 6.1319),
    "Malta": (35.8997, 14.5146),
    "Netherlands": (52.3676, 4.9041),
    "North Macedonia": (41.9981, 21.4254),
    "Norway": (59.9139, 10.7522),
    "Poland": (52.2297, 21.0122),
    "Portugal": (38.7169, -9.1392),
    "Romania": (44.4268, 26.1025),
    "San Marino": (43.9336, 12.4488),
    "Serbia": (44.8176, 20.4569),
    "Slovakia": (48.1486, 17.1077),
    "Slovenia": (46.0569, 14.5058),
    "Spain": (40.4168, -3.7038),
    "Sweden": (59.3293, 18.0686),
    "Switzerland": (46.9481, 7.4474),
    "Ukraine": (50.4501, 30.5234),
    "United Kingdom": (51.5074, -0.1278)
}

# Polluants
variables = ["pm2_5", "pm10", "nitrogen_dioxide", "ozone"]

# Années pour lesquelles on veut récupérer les données
current_year = datetime.now().year
today = datetime.now().strftime("%Y-%m-%d")
years = [2023, 2024, current_year]

all_data = []

for country, coords in countries.items():
    lat, lon = coords
    for year in years:
        start_date = f"{year}-01-01"
        # Limiter la date de fin à aujourd'hui pour l'année en cours
        end_date = today if year == current_year else f"{year}-12-31"
        url = (
            f"https://air-quality-api.open-meteo.com/v1/air-quality?"
            f"latitude={lat}&longitude={lon}&start_date={start_date}&end_date={end_date}"
            f"&hourly={','.join(variables)}"
        )
        print(f"Fetching {country} for {year} …")
        try:
            resp = requests.get(url)
            resp.raise_for_status()
            data = resp.json()
            if "hourly" not in data or not data["hourly"]:
                print(f"Aucune donnée pour {country} en {year}")
                continue
            df = pd.DataFrame(data["hourly"])
            df["time"] = pd.to_datetime(df["time"])
            df["country"] = country
            df["year"] = year
            all_data.append(df)
            time.sleep(1)
        except requests.exceptions.HTTPError as e:
            print(f"Erreur HTTP pour {country} en {year}: {e}")
        except Exception as e:
            print(f"Erreur inattendue pour {country} en {year}: {e}")

if all_data:
    df_all = pd.concat(all_data, ignore_index=True)
    df_all['month'] = df_all['time'].dt.to_period('M')
    monthly_avg = df_all.groupby(['country', 'year', 'month'])[variables].mean().reset_index()
    monthly_avg.to_csv("air_quality_europe_monthly_avg.csv", index=False)
    print("Fichier CSV sauvegardé avec les moyennes mensuelles.")
else:
    print("Aucune donnée récupérée.")
