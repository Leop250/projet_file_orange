import requests
import os
import csv
import time
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry

token = "8d5c907af6d2a3e0151937893c7f100430cdf09a"
all_stations = []

def get_stations_in_bounds(token, latlng_bounds):
    """
    Récupère les stations dans une zone géographique définie par latlng_bounds.

    :param token: clé API AQICN
    :param latlng_bounds: chaîne format "lat1,lng1,lat2,lng2" représentant la bounding box
    :return: liste des stations dans la zone, ou None en cas d'erreur
    """
    url = f"https://api.waqi.info/map/bounds/"
    params = {
        'token': token,
        'latlng': latlng_bounds
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        if data.get('status') == 'ok':
            return data.get('data', [])
    return None


# Exemple d’utilisation :
# Définition de plusieurs zones (latitude_1, longitude_1, latitude_2, longitude_2)
zones = [
    # Europe
    "34.5,-11.5,71.0,40.0"
]

for zone in zones:
    stations = get_stations_in_bounds(token, zone)
    if stations:
        all_stations.extend(stations)
    else:
        print(f"Erreur ou pas de stations dans la zone {zone}")

print(f"Nombre total de stations récupérées : {len(all_stations)}")
# Vous pouvez maintenant traiter all_stations comme une liste d'objets stations


def recuperer_aqi_pour_villes_detail(all_stations, token, output_csv):
    url_template = "https://api.waqi.info/feed/{}/?token={}"

    # Colonnes fixes
    champs = [
        'idx', 'station_name', 'station_url', 'aqi',
        'time_v', 'time_s', 'time_tz',
        'lat', 'lon'
    ]

    # Ajouter colonnes polluants dynamiquement (liste courante commune)
    polluants = ['pm25', 'pm10', 'no2', 'co', 'so2', 'o3', 't', 'w', 'r']  # t: temp, w: wind, r: rain
    for p in polluants:
        champs.append(f'iaqi_{p}')

    # Ajouter colonnes prévisions PM2.5 (2 jours max)
    for day_index in range(2):
        champs += [
            f'forecast_pm25_day{day_index + 1}_date',
            f'forecast_pm25_day{day_index + 1}_avg',
            f'forecast_pm25_day{day_index + 1}_max',
            f'forecast_pm25_day{day_index + 1}_min'
        ]

    with open(output_csv, mode='w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=champs)
        writer.writeheader()

        for i, station in enumerate(all_stations, 1):
            nom_station = station.get('station', {}).get('name') or station.get('station_name')
            if not nom_station:
                print(f"[{i}] Station sans nom, sautée")
                continue

            print(f"[{i}] Récupération des données pour : {nom_station} ...")
            url = url_template.format(nom_station.replace(' ', '%20'), token)
            try:
                response = requests.get(url)
                data = response.json()

                if data['status'] == 'ok':
                    d = data['data']
                    idx = d.get('idx')
                    aqi = d.get('aqi')
                    time_data = d.get('time', {})
                    city_data = d.get('city', {})
                    iaqi = d.get('iaqi', {})
                    forecast_pm25 = d.get('forecast', {}).get('daily', {}).get('pm25', [])

                    ligne = {
                        'idx': idx,
                        'station_name': city_data.get('name'),
                        'station_url': city_data.get('url'),
                        'aqi': aqi,
                        'time_v': time_data.get('v'),
                        'time_s': time_data.get('s'),
                        'time_tz': time_data.get('tz'),
                        'lat': city_data.get('geo', [None, None])[0],
                        'lon': city_data.get('geo', [None, None])[1],
                    }

                    # Détail des polluants (valeurs si présentes)
                    for p in polluants:
                        ligne[f'iaqi_{p}'] = iaqi.get(p, {}).get('v')

                    # Prévisions PM2.5 pour 2 jours max
                    for day_index in range(2):
                        if day_index < len(forecast_pm25):
                            day = forecast_pm25[day_index]
                            ligne[f'forecast_pm25_day{day_index + 1}_date'] = day.get('day')
                            ligne[f'forecast_pm25_day{day_index + 1}_avg'] = day.get('avg')
                            ligne[f'forecast_pm25_day{day_index + 1}_max'] = day.get('max')
                            ligne[f'forecast_pm25_day{day_index + 1}_min'] = day.get('min')
                        else:
                            ligne[f'forecast_pm25_day{day_index + 1}_date'] = None
                            ligne[f'forecast_pm25_day{day_index + 1}_avg'] = None
                            ligne[f'forecast_pm25_day{day_index + 1}_max'] = None
                            ligne[f'forecast_pm25_day{day_index + 1}_min'] = None

                    writer.writerow(ligne)
                    print(f"[{i}] Données enregistrées pour : {nom_station}")

                else:
                    print(f"[{i}] Erreur API pour {nom_station}: {data.get('data')}")

            except Exception as e:
                print(f"[{i}] Exception pour {nom_station} : {e}")

            time.sleep(1)

recuperer_aqi_pour_villes_detail(all_stations, token, "resultats_stations_detail_histo.csv")
print("Fin du traitement.")
