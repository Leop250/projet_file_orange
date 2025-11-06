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

# Seuils de qualité de l'air (OMS et EPA)
AIR_QUALITY_THRESHOLDS = {
    "pm25": {"bon": 12, "moyen": 35, "mauvais": 55},
    "pm10": {"bon": 50, "moyen": 100, "mauvais": 150},
    "o3": {"bon": 100, "moyen": 160, "mauvais": 240},
    "no2": {"bon": 40, "moyen": 100, "mauvais": 200},
    "so2": {"bon": 20, "moyen": 80, "mauvais": 250},
    "co": {"bon": 4000, "moyen": 9000, "mauvais": 15000}
}


def test_api_connection(api_key):
    """Teste la connexion à l'API."""
    headers = {"X-API-Key": api_key}
    url = "https://api.openaq.org/v3/locations"
    params = {"limit": 1}

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        print(f"🔍 Test API: Status {resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            print(f"✅ API fonctionnelle - {data.get('meta', {}).get('found', 0)} locations disponibles")
            return True
        else:
            print(f"❌ Erreur API: {resp.text}")
            return False
    except Exception as e:
        print(f"❌ Erreur de connexion: {e}")
        return False


def get_country_data_direct(country_code, parameter, api_key, limit=100):
    """
    Méthode alternative: récupère directement les mesures les plus récentes.
    """
    country_id = COUNTRY_IDS.get(country_code)
    param_id = PARAMETER_IDS.get(parameter.lower(), 2)

    headers = {"X-API-Key": api_key}
    url = "https://api.openaq.org/v3/locations"

    params = {
        "countries_id": country_id,
        "parameters_id": param_id,
        "limit": limit,
        "sort": "desc",
        "order_by": "lastUpdated"
    }

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=15)

        if resp.status_code != 200:
            print(f"    ⚠️  Status {resp.status_code}: {resp.text[:100]}")
            return []

        locations = resp.json().get("results", [])

        if not locations:
            return []

        # Extraire les valeurs des sensors
        values = []
        for loc in locations:
            for sensor in loc.get("sensors", []):
                if sensor.get("parameter", {}).get("id") == param_id:
                    latest = sensor.get("latest", {})
                    value = latest.get("value")

                    if value is not None:
                        # Filtrage basique
                        if parameter == "co" and -100 < value < 50000:
                            values.append(value)
                        elif parameter in ["pm25", "pm10", "o3", "no2", "so2"] and -10 < value < 2000:
                            values.append(value)

        return values

    except Exception as e:
        print(f"    ❌ Exception: {e}")
        return []


def get_parameter_stats(country_code, parameter, api_key):
    """Récupère les statistiques pour un polluant dans un pays."""
    values = get_country_data_direct(country_code, parameter, api_key, limit=200)

    if not values or len(values) == 0:
        return None

    # Calcul des statistiques
    values_sorted = sorted(values)
    n = len(values_sorted)

    return {
        "moyenne": sum(values) / n,
        "mediane": values_sorted[n // 2] if n % 2 == 1 else (values_sorted[n // 2 - 1] + values_sorted[n // 2]) / 2,
        "nb_mesures": n,
        "min": min(values),
        "max": max(values)
    }


def get_air_quality_level(parameter, value):
    """Détermine le niveau de qualité de l'air."""
    if value is None:
        return "N/A"

    thresholds = AIR_QUALITY_THRESHOLDS.get(parameter, {})
    if value < thresholds.get("bon", float('inf')):
        return "🟢 Bon"
    elif value < thresholds.get("moyen", float('inf')):
        return "🟡 Moyen"
    elif value < thresholds.get("mauvais", float('inf')):
        return "🟠 Mauvais"
    else:
        return "🔴 Très mauvais"


def analyze_all_pollutants(countries, api_key):
    """Analyse tous les polluants pour une liste de pays."""
    results = []

    for country in countries:
        print(f"\n{'=' * 60}")
        print(f"🌍 Analyse de {country}")
        print('=' * 60)

        country_data = {"Pays": country}
        has_data = False

        for param_name in PARAMETER_IDS.keys():
            print(f"  📊 {param_name.upper()}...", end=" ", flush=True)

            stats = get_parameter_stats(country, param_name, api_key)

            if stats:
                avg = stats["moyenne"]
                med = stats["mediane"]
                quality = get_air_quality_level(param_name, avg)

                country_data[f"{param_name}_moyenne"] = round(avg, 2)
                country_data[f"{param_name}_mediane"] = round(med, 2)
                country_data[f"{param_name}_qualite"] = quality
                country_data[f"{param_name}_nb"] = stats["nb_mesures"]

                print(f"✅ Moy: {avg:.1f}, Méd: {med:.1f} {quality} ({stats['nb_mesures']} mesures)")
                has_data = True
            else:
                country_data[f"{param_name}_moyenne"] = None
                country_data[f"{param_name}_mediane"] = None
                country_data[f"{param_name}_qualite"] = "N/A"
                country_data[f"{param_name}_nb"] = 0
                print("❌ Pas de données")

            time.sleep(0.2)  # Pause entre chaque requête

        if has_data:
            results.append(country_data)
        else:
            print(f"  ⚠️  Aucune donnée disponible pour {country}")

    return pd.DataFrame(results)


def create_summary_table(df):
    """Crée un tableau résumé avec les moyennes par polluant."""
    summary_rows = []

    for _, row in df.iterrows():
        for param in PARAMETER_IDS.keys():
            if row[f"{param}_moyenne"] is not None:
                summary_rows.append({
                    "Pays": row["Pays"],
                    "Polluant": param.upper(),
                    "Moyenne": row[f"{param}_moyenne"],
                    "Médiane": row[f"{param}_mediane"],
                    "Qualité": row[f"{param}_qualite"],
                    "Nb mesures": row[f"{param}_nb"]
                })

    return pd.DataFrame(summary_rows)


def estimate_overall_quality(row):
    """Estime la qualité globale de l'air pour un pays."""
    qualities = []
    weights = {"pm25": 3, "pm10": 2, "o3": 2, "no2": 2, "so2": 1, "co": 1}

    for param in PARAMETER_IDS.keys():
        if row[f"{param}_moyenne"] is not None:
            avg = row[f"{param}_moyenne"]
            quality = get_air_quality_level(param, avg)

            # Score: Bon=1, Moyen=2, Mauvais=3, Très mauvais=4
            if "Bon" in quality:
                score = 1
            elif "Moyen" in quality:
                score = 2
            elif "Mauvais" in quality and "Très" not in quality:
                score = 3
            else:
                score = 4

            qualities.append(score * weights.get(param, 1))

    if not qualities:
        return "N/A"

    avg_score = sum(qualities) / sum(weights.values())

    if avg_score < 1.5:
        return "🟢 Bonne"
    elif avg_score < 2.5:
        return "🟡 Moyenne"
    elif avg_score < 3.5:
        return "🟠 Mauvaise"
    else:
        return "🔴 Très mauvaise"


if __name__ == "__main__":
    try:
        api_key = os.getenv("OPENAQ_API_KEY")
        if not api_key:
            raise ValueError("Aucune clé API trouvée. Vérifie que OPENAQ_API_KEY est bien définie.")

        print("\n" + "=" * 60)
        print("📊 ANALYSE MULTI-POLLUANTS PAR PAYS")
        print("=" * 60)

        # Test de connexion
        if not test_api_connection(api_key):
            raise Exception("Impossible de se connecter à l'API OpenAQ")

        # Liste des pays à analyser
        pays = ["FR", "US", "IN", "GB", "DE", "ES"]

        # Analyse complète
        df_complete = analyze_all_pollutants(pays, api_key)

        if df_complete.empty:
            print("\n❌ Aucune donnée récupérée pour aucun pays.")
            exit(1)

        # Ajout de la qualité globale
        df_complete["Qualité globale"] = df_complete.apply(estimate_overall_quality, axis=1)

        # Tableau résumé
        print("\n" + "=" * 60)
        print("📋 TABLEAU RÉCAPITULATIF PAR POLLUANT")
        print("=" * 60)
        df_summary = create_summary_table(df_complete)

        if not df_summary.empty:
            print(df_summary.to_string(index=False))
        else:
            print("⚠️  Pas de données à afficher")

        # Sauvegarde en CSV
        if not df_complete.empty:
            df_complete.to_csv("qualite_air_complete.csv", index=False)
            print("\n✅ Fichier sauvegardé: qualite_air_complete.csv")

        if not df_summary.empty:
            df_summary.to_csv("qualite_air_resume.csv", index=False)
            print("✅ Fichier sauvegardé: qualite_air_resume.csv")

        # Vue d'ensemble par pays
        print("\n" + "=" * 60)
        print("🌍 QUALITÉ DE L'AIR PAR PAYS")
        print("=" * 60)
        overview_cols = ["Pays", "pm25_moyenne", "pm10_moyenne", "o3_moyenne", "no2_moyenne", "Qualité globale"]
        available_cols = [col for col in overview_cols if col in df_complete.columns]
        print(df_complete[available_cols].to_string(index=False))

    except Exception as e:
        print(f"\n❌ Erreur : {e}")
        import traceback

        traceback.print_exc()