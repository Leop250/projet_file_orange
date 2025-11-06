import os
import requests
import pandas as pd
from datetime import datetime
import time

# Mapping des codes pays vers leurs IDs OpenAQ
COUNTRY_IDS = {
    "FR": 71,  # France
    "GH": 152,  # Ghana
    "IN": 9,  # India
    "US": 219,  # United States
    "GB": 223,  # United Kingdom
    "DE": 49,  # Germany
    "ES": 67,  # Spain
    "IT": 98,  # Italy
    "CN": 45,  # China
    "JP": 103,  # Japan
}

# IDs des paramètres OpenAQ
PARAMETER_IDS = {
    "pm25": 2,
    "pm10": 1,
    "o3": 5,
    "no2": 3,
    "so2": 4,
    "co": 6
}


def get_country_sensors(country_code="FR", parameter="pm25", api_key=None, limit=100):
    """
    Récupère la liste des sensor IDs pour un pays.
    """
    if api_key is None:
        api_key = os.getenv("OPENAQ_API_KEY")
        if not api_key:
            raise ValueError("Une clé API OpenAQ doit être définie dans OPENAQ_API_KEY")

    country_id = COUNTRY_IDS.get(country_code)
    if not country_id:
        raise ValueError(f"Code pays '{country_code}' non supporté.")

    param_id = PARAMETER_IDS.get(parameter.lower(), 2)

    headers = {"X-API-Key": api_key}
    url = "https://api.openaq.org/v3/locations"

    params = {
        "countries_id": country_id,
        "parameters_id": param_id,
        "limit": limit
    }

    resp = requests.get(url, headers=headers, params=params)

    if resp.status_code != 200:
        print(f"⚠️  Erreur API: {resp.status_code}")
        return []

    locations = resp.json().get("results", [])

    # Extraire les sensor IDs avec leurs infos de location
    sensors = []
    for loc in locations:
        location_name = loc.get("name", "Inconnue")
        city = loc.get("locality") or loc.get("city") or "Inconnue"
        country_info = loc.get("country", {})
        coords = loc.get("coordinates", {})

        for sensor in loc.get("sensors", []):
            if sensor.get("parameter", {}).get("id") == param_id:
                sensors.append({
                    "sensor_id": sensor.get("id"),
                    "location_name": location_name,
                    "city": city,
                    "country": country_info.get("name", country_code),
                    "latitude": coords.get("latitude"),
                    "longitude": coords.get("longitude"),
                })

    return sensors


def get_sensor_latest(sensor_id, api_key):
    """
    Récupère la dernière mesure d'un sensor spécifique.
    """
    headers = {"X-API-Key": api_key}
    url = f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements"

    params = {
        "limit": 1,
        "sort": "desc",
        "order_by": "datetime"
    }

    resp = requests.get(url, headers=headers, params=params)

    if resp.status_code != 200:
        return None

    results = resp.json().get("results", [])
    return results[0] if results else None


def get_country_air_quality(country_code="FR", parameter="pm25", api_key=None, max_sensors=100):
    """
    Récupère les dernières mesures pour un pays.
    """
    if api_key is None:
        api_key = os.getenv("OPENAQ_API_KEY")
        if not api_key:
            raise ValueError("Une clé API OpenAQ doit être définie dans OPENAQ_API_KEY")

    print(f"🌍 Récupération des capteurs pour {country_code}...")
    sensors = get_country_sensors(country_code, parameter, api_key, limit=200)

    if not sensors:
        print(f"⚠️  Aucun capteur trouvé pour {country_code}")
        return pd.DataFrame()

    print(f"📡 {len(sensors)} capteurs trouvés")

    # Limiter le nombre de sensors
    sensors = sensors[:max_sensors]

    # Récupérer les mesures une par une
    print(f"📊 Récupération des mesures (cela peut prendre un moment)...")

    rows = []
    for i, sensor in enumerate(sensors):
        if (i + 1) % 10 == 0:
            print(f"   Progression: {i + 1}/{len(sensors)}")

        measurement = get_sensor_latest(sensor["sensor_id"], api_key)

        if measurement:
            value = measurement.get("value")

            # Filtrer les valeurs aberrantes
            if value is not None and -100 < value < 1000:
                dt_obj = measurement.get("date", {})
                dt = dt_obj.get("utc") if isinstance(dt_obj, dict) else dt_obj

                rows.append({
                    "datetime": dt,
                    "city": sensor["city"],
                    "location_name": sensor["location_name"],
                    "parameter": parameter,
                    "value": value,
                    "unit": "µg/m³",
                    "latitude": sensor["latitude"],
                    "longitude": sensor["longitude"],
                })

        # Petite pause pour ne pas surcharger l'API
        if i < len(sensors) - 1:
            time.sleep(0.1)

    if not rows:
        print(f"⚠️  Aucune mesure valide pour {country_code}")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Convertir datetime
    if not df.empty and "datetime" in df.columns:
        try:
            df["datetime"] = pd.to_datetime(df["datetime"])
        except:
            pass

    print(f"✅ {len(df)} mesures récupérées")
    return df


def get_country_summary(country_code="FR", parameter="pm25", api_key=None):
    """
    Obtient un résumé statistique de la qualité de l'air pour un pays.
    """
    df = get_country_air_quality(country_code, parameter, api_key=api_key, max_sensors=100)

    if df.empty:
        return None

    summary = {
        "pays": country_code,
        "polluant": parameter,
        "nombre_stations": len(df),
        "moyenne": df["value"].mean(),
        "mediane": df["value"].median(),
        "minimum": df["value"].min(),
        "maximum": df["value"].max(),
        "ecart_type": df["value"].std(),
        "villes_uniques": df[df["city"] != "Inconnue"]["city"].nunique(),
        "dernière_mesure": df["datetime"].max() if not df.empty else None
    }

    return summary


def list_available_countries():
    """Affiche la liste des pays disponibles."""
    print("\n📋 Pays disponibles:")
    for code, country_id in sorted(COUNTRY_IDS.items()):
        print(f"  • {code} (ID: {country_id})")


if __name__ == "__main__":
    try:
        api_key = os.getenv("OPENAQ_API_KEY")
        if not api_key:
            raise ValueError("Aucune clé API trouvée. Vérifie que OPENAQ_API_KEY est bien définie.")

        list_available_countries()

        # Liste des pays à analyser
        pays = ["FR", "US", "IN", "GB", "DE", "ES"]

        print("\n" + "=" * 60)
        print("📊 ANALYSE DE LA QUALITÉ DE L'AIR PAR PAYS (PM2.5)")
        print("=" * 60)

        resultats = []

        for country in pays:
            print(f"\n{'=' * 60}")
            summary = get_country_summary(country, parameter="pm25", api_key=api_key)

            if summary:
                qualite = "🟢 Bon" if summary['moyenne'] < 12 else "🟡 Moyen" if summary[
                                                                                   'moyenne'] < 35 else "🟠 Mauvais" if \
                summary['moyenne'] < 55 else "🔴 Très mauvais"
                print(
                    f"✅ {summary['pays']}: {summary['moyenne']:.1f} µg/m³ {qualite} (médiane: {summary['mediane']:.1f})")
                print(f"   📍 {summary['nombre_stations']} stations", end="")
                if summary['villes_uniques'] > 0:
                    print(f" | 🏙️  {summary['villes_uniques']} villes")
                else:
                    print()
                print(f"   📉 Min: {summary['minimum']:.1f} | 📈 Max: {summary['maximum']:.1f}")
                resultats.append(summary)
            else:
                print(f"⚠️  Pas de données pour {country}")

        # Tableau comparatif
        if resultats:
            print(f"\n{'=' * 60}")
            print("📈 TABLEAU COMPARATIF")
            print("=" * 60)
            df_compare = pd.DataFrame(resultats)
            df_compare = df_compare.sort_values('moyenne', ascending=False)
            print(
                df_compare[['pays', 'nombre_stations', 'moyenne', 'mediane', 'minimum', 'maximum']].round(1).to_string(
                    index=False))

            # Analyse détaillée pour la France
            print(f"\n{'=' * 60}")
            print("📋 TOP 10 STATIONS LES PLUS POLLUÉES EN FRANCE")
            print("=" * 60)
            df_france = get_country_air_quality("FR", parameter="pm25", api_key=api_key, max_sensors=100)

            if not df_france.empty:
                df_france = df_france.sort_values('value', ascending=False)
                print(df_france[["city", "location_name", "value", "datetime"]].head(10).to_string(index=False))

    except Exception as e:
        print(f"❌ Erreur : {e}")
        import traceback

        traceback.print_exc()