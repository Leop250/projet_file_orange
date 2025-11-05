import requests
import os

token = os.environ["TOKEN_AQICN"]


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
    # New York
    "40.477,-74.259,40.917,-73.700",
    # Los Angeles
    "33.703,-118.668,34.337,-118.155",
    # London
    "51.286,-0.515,51.691,0.236",
    # Paris
    "48.816,2.224,48.902,2.469",
    # Tokyo
    "35.527,139.651,35.929,139.921",
    # Beijing
    "39.403,115.490,39.961,116.410",
    # Mumbai
    "18.889,72.753,19.218,72.998",
    # São Paulo
    "-23.385,-46.897,-23.114,-46.387",
    # Sydney
    "-34.118,151.701,-33.702,151.343",
    # Moscou
    "55.532,37.332,55.907,37.898",
    # Le Cap (Cape Town)
    "-34.129,18.367,-33.726,18.655",
    # Cairo
    "30.477,31.219,30.833,31.552",
]

all_stations = []

for zone in zones:
    stations = get_stations_in_bounds(token, zone)
    if stations:
        all_stations.extend(stations)
    else:
        print(f"Erreur ou pas de stations dans la zone {zone}")

print(f"Nombre total de stations récupérées : {len(all_stations)}")
# Vous pouvez maintenant traiter all_stations comme une liste d'objets stations

