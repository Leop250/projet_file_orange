import os
import requests
import pandas as pd

def get_openaq_measurements(
    sensor_id: int = None,
    location_id: int = None,
    parameter_id: int = None,
    datetime_from: str = None,
    datetime_to: str = None,
    limit: int = 100,
    page: int = 1,
    api_key: str = None
) -> pd.DataFrame:
    """
    Récupère des mesures depuis l’API OpenAQ v3.
    On peut filtrer par sensor_id, location_id, parameter_id, plage temporelle.
    """
    if api_key is None:
        api_key = os.getenv("OPENAQ_API_KEY")
        if api_key is None:
            raise ValueError("Une clé API OpenAQ doit être fournie ou définie dans OPENAQ_API_KEY")

    headers = {
        "X-API-Key": api_key
    }

    # Choisir l’endpoint selon ce que tu veux
    # Ici on utilise sensor_id si fourni, sinon on pourrait utiliser d'autres endpoints
    if sensor_id:
        url = f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements"
    else:
        # Autre endpoint générique : on utilise /v3/measurements n’est pas documenté comme générique
        url = "https://api.openaq.org/v3/sensors/measurements"  # **à adapter** selon ton besoin
        # Note : la doc indique /v3/sensors/{sensors_id}/measurements pour les mesures par capteur. :contentReference[oaicite:1]{index=1}

    params = {
        "limit": limit,
        "page": page
    }
    if datetime_from:
        params["datetime_from"] = datetime_from
    if datetime_to:
        params["datetime_to"] = datetime_to
    if parameter_id:
        params["parameter_id"] = parameter_id
    if location_id:
        params["locations_id"] = location_id  # attention nom exact selon doc

    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        raise RuntimeError(f"Erreur API OpenAQ : {response.status_code} {response.text}")

    json_data = response.json()
    results = json_data.get("results", [])
    df = pd.json_normalize(results)
    return df

# Exemple d’appel
if __name__ == "__main__":
    API_KEY = "clé api"
    try:
        df = get_openaq_measurements(
            sensor_id=3917,
            parameter_id=2,
            datetime_from="2025-10-01T00:00:00Z",
            datetime_to="2025-10-31T23:59:59Z",
            limit=50,
            api_key=API_KEY
        )
        print(df.head())
    except Exception as e:
        print("Erreur :", e)
