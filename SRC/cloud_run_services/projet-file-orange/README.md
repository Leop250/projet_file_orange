# Air Quality Monitoring Platform

Plateforme de collecte, de traitement et de visualisation de données de qualité de l’air pour plusieurs grandes villes européennes via l’API OpenAQ (observations) et Open-Meteo (prévisions), deux services open data gratuits.

## Prérequis
- Python 3.10+
- `pip` ou `pipenv`
- Jeton API OpenAQ (`OPENAQ_API_KEY`)
- Accès internet pour interroger OpenAQ et Open-Meteo

## Installation rapide

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export OPENAQ_API_KEY="votre_clé_api"
```

Les données sont stockées par défaut dans `./data/air_quality.db` (SQLite) et les graphiques sont exportés dans `./artifacts/`.

## Utilisation

### Ingestion + visualisation
```bash
python main.py
```

### Options utiles
- `--cities Paris London` : restreindre la collecte à certaines villes prédéfinies.
- `--city-overrides cities.json` : ajouter des villes personnalisées (fichier JSON cf. exemple ci-dessous).
- `--skip-ingest` : uniquement générer des visualisations depuis la base existante.
- `--skip-visuals` : uniquement ingérer/mettre à jour les données.
- `--skip-forecast` : ignorer Open-Meteo et ne collecter que les observations OpenAQ.
- `--forecast-hours 120` : ajuster l’horizon de prévision (par défaut 72 h, max 168 h).
- `--log-level DEBUG` : augmenter la verbosité.

### Exemple de fichier `cities.json`
```json
{
  "Zurich": {
    "country": "Switzerland",
    "latitude": 47.3769,
    "longitude": 8.5417
  },
  "Lisbon": {
    "country": "Portugal",
    "latitude": 38.7223,
    "longitude": -9.1393
  }
}
```

## Visualisations générées
- Séries temporelles des concentrations de polluants (PM2.5, PM10, NO₂, O₃, SO₂, CO) par ville, mêlant observations OpenAQ et prévisions Open-Meteo.
- Histogramme des concentrations moyennes des polluants (PM2.5, PM10, NO₂, O₃, SO₂, CO) sur les 7 derniers jours disponibles.

## Personnalisation
- `AIR_QUALITY_DB_PATH` : changer le chemin de la base SQLite.
- `AIR_QUALITY_ARTIFACTS_DIR` : dossier de sortie pour les graphiques.
- `OPENAQ_API_KEY` : jeton OpenAQ (obtenu gratuitement sur https://docs.openaq.org/).

## Étapes suivantes possibles
- Automatisation (cron/Airflow) pour rafraîchir les données à intervalles réguliers.
- Intégration dans un tableau de bord (Streamlit, Dash, Looker Studio).
- Ajout de notifications (mail, SMS) lorsque les concentrations dépassent des seuils réglementaires.
