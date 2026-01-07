# AeroScope - Tableau de Bord de Surveillance Environnementale

[![Next.js](https://img.shields.io/badge/Next.js-16.0-black?style=flat&logo=next.js)](https://nextjs.org/)
[![Python](https://img.shields.io/badge/Python-3.11-blue?style=flat&logo=python)](https://www.python.org/)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.x-blue?style=flat&logo=typescript)](https://www.typescriptlang.org/)
[![Google Cloud](https://img.shields.io/badge/Google%20Cloud-BigQuery-4285F4?style=flat&logo=google-cloud)](https://cloud.google.com/bigquery)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Plateforme full-stack pour la visualisation en temps réel des données de qualité de l'air et météorologiques de plus de 100 villes dans le monde, avec pipelines ETL automatisés et tableau de bord web interactif.

---

## Table des Matières

- [Vue d'Ensemble](#vue-densemble)
- [Architecture Système](#architecture-système)
- [Stack Technologique](#stack-technologique)
- [Structure du Projet](#structure-du-projet)
- [Prérequis](#prérequis)
- [Installation](#installation)
  - [Configuration Backend (ETL Python)](#configuration-backend-etl-python)
  - [Configuration Frontend (Next.js)](#configuration-frontend-nextjs)
- [Configuration](#configuration)

---

## Vue d'Ensemble

**AeroScope** est une plateforme moderne de surveillance environnementale de qualité production qui combine des données de qualité de l'air en temps réel (PM2.5, PM10, NO₂, O₃, SO₂, CO, NH₃) avec des informations météorologiques (température, humidité, vent) pour fournir des aperçus environnementaux urbains complets.

### Fonctionnalités Clés

- **Surveillance en Temps Réel** : Données de qualité de l'air et météo en direct pour plus de 100 villes
- **Analyse Historique** : 24 mois de données historiques avec visualisation des tendances
- **Cartographie Interactive** : Intégration Google Maps pour l'exploration spatiale des données
- **Pipeline ETL Automatisé** : Ingestion de données cloud depuis plusieurs APIs
- **Architecture Type-Safe** : Implémentation TypeScript complète avec typage strict
- **Infrastructure Scalable** : Architecture serverless sur Google Cloud Platform
- **Performance Optimisée** : Temps de chargement < 3 secondes avec requêtes BigQuery efficaces

### Cas d'Usage

- **Santé Publique** : Surveiller la qualité de l'air avant des activités extérieures
- **Recherche** : Analyser les tendances de pollution et corrélations
- **Élaboration de Politiques** : Évaluer l'efficacité des politiques environnementales
- **Urbanisme** : Décisions de développement urbain basées sur les données

---

## Architecture Système

### Vue d'Ensemble Haut Niveau

```
┌──────────────────────────────────────────────────────────────┐
│                  SOURCES DE DONNÉES EXTERNES                 │
│  • WeatherAPI.com (Données météo actuelles)                  │
│  • OpenWeatherMap (API qualité de l'air)                     │
│  • Open-Meteo (Qualité de l'air historique)                  │
│  • OpenAQ (Données de pollution alternatives)                │
└────────────────────┬─────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│            PIPELINE ETL (Python/Flask/Cloud Run)            │
│                                                             │
│  ┌─────────────────────────────────────────────┐            │
│  │  Services de Collection de Données          │            │
│  │  • weatherapicom (main.py)                  │            │
│  │  • openaq-collector (main.py)               │            │
│  │  • airqualitynew (main.py)                  │            │
│  │  • qualiteair (main.py)                     │            │
│  └────────────────┬────────────────────────────┘            │
│                   │                                         │
│  ┌────────────────▼────────────────────────────┐            │
│  │  Couche de Traitement des Données           │            │
│  │  • Extraction depuis APIs REST              │            │
│  │  • Transformation & nettoyage (pandas)      │            │
│  │  • Validation & normalisation               │            │
│  │  • Gestion d'erreurs & logging              │            │
│  └────────────────┬────────────────────────────┘            │
└───────────────────┼─────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────┐
│          GOOGLE BIGQUERY (Entrepôt de Données)              │
│                                                             │
│  Tables :                                                   │
│  • weather_data.weather_records                             │
│    → Météo en temps réel par ville                          │
│  • weather_data.weather_monthly_avg                         │
│    → Agrégats météo historiques par pays                    │
│  • weather_data.weatherapicom                               │
│    → Données brutes WeatherAPI.com                          │
│  • airquality_full.measurements_complette                   │
│    → Mesures de pollution complètes                         │
│  • air_quality_europe_monthly_avg.donne_open_meteo_*        │
│    → Données historiques Open-Meteo                         │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│         APPLICATION WEB (Next.js 16 + React 19)             │
│                                                             │
│  ┌──────────────────────────────────────────────┐           │
│  │  Couche Backend (Routes API)                 │           │
│  │                                              │           │
│  │  /api/weather_data/route.ts                  │           │
│  │  • Authentification Google Cloud             │           │
│  │  • Requêtes BigQuery optimisées              │           │
│  │  • Fusion de données multi-sources           │           │
│  │  • Mise en cache & optimisation réponses     │           │
│  └────────────────┬─────────────────────────────┘           │
│                   │                                         │
│  ┌────────────────▼─────────────────────────────┐           │
│  │  Couche Frontend (Composants React)          │           │
│  │                                              │           │
│  │  app/page.tsx                                │           │
│  │  • Orchestration du tableau de bord          │           │
│  │  • Gestion d'état                            │           │
│  │  • Récupération & cache des données          │           │
│  │                                              │           │
│  │  components/                                 │           │
│  │  • google_map_component.tsx                  │           │
│  │    → Intégration Google Maps interactive     │           │
│  │    → Mise à jour des marqueurs en temps réel │           │
│  │  • air_quality_chart_component.tsx           │           │
│  │    → Visualisations Recharts                 │           │
│  │    → Analyse des tendances historiques       │           │
│  │                                              │           │
│  │  types/index.ts                              │           │
│  │  • Interfaces TypeScript                     │           │
│  │  • Définitions de types pour réponses API    │           │
│  │  • Modèles de données partagés               │           │
│  └──────────────────────────────────────────────┘           │
└─────────────────────────────────────────────────────────────┘
```

### Flux de Données

1. **Ingestion** : Les services ETL Python collectent les données depuis les APIs externes à intervalles programmés
2. **Stockage** : Données structurées chargées dans BigQuery avec partitionnement par date
3. **Requête** : Les routes API Next.js exécutent des requêtes SQL BigQuery optimisées
4. **Transformation** : Fusion des données côté serveur combinant météo + pollution
5. **Rendu** : Les composants React visualisent les données avec graphiques et cartes interactifs

---

## Stack Technologique

### Backend (Pipeline ETL)

| Technologie | Version | Objectif |
|-----------|---------|---------|
| [Python](https://www.python.org/) | 3.11 | Langage de script principal |
| [Flask](https://flask.palletsprojects.com/) | Latest | Framework serveur HTTP pour microservices |
| [pandas](https://pandas.pydata.org/) | Latest | Manipulation et transformation de données |
| [matplotlib](https://matplotlib.org/) | Latest | Génération visualisations (graphiques PNG) |
| [google-cloud-bigquery](https://cloud.google.com/python/docs/reference/bigquery/latest) | Latest | Client Python BigQuery |
| [requests](https://requests.readthedocs.io/) | Latest | Client HTTP pour APIs externes |
| [Google Cloud Run](https://cloud.google.com/run) | - | Hébergement conteneurs serverless |
| [Cloud Build](https://cloud.google.com/build) | - | Automatisation CI/CD depuis GitHub |
| [Cloud Scheduler](https://cloud.google.com/scheduler) | - | Déclenchement pipelines ETL (cron jobs) |

### Frontend (Application Web)

| Technologie | Version | Objectif |
|-----------|---------|---------|
| [Next.js](https://nextjs.org/) | 16.0 | Framework React avec SSR/SSG |
| [React](https://react.dev/) | 19.x | Bibliothèque UI basée composants |
| [TypeScript](https://www.typescriptlang.org/) | 5.x | Typage statique & sécurité |
| [Tailwind CSS](https://tailwindcss.com/) | 4.x | Framework CSS utility-first |
| [Recharts](https://recharts.org/) | 3.6+ | Bibliothèque de graphiques SVG |
| [@google-cloud/bigquery](https://www.npmjs.com/package/@google-cloud/bigquery) | 8.1+ | Client BigQuery Node.js |
| [@react-google-maps/api](https://www.npmjs.com/package/@react-google-maps/api) | 2.20+ | Wrapper React Google Maps |

### Infrastructure

| Service | Objectif |
|---------|---------|
| Google BigQuery | Entrepôt de données scalable |
| Google Cloud Storage | Stockage d'artefacts |
| Google Cloud Run | Hébergement ETL serverless |
| Google Cloud Build | Déploiements automatisés |
| Google Cloud IAM | Contrôle d'accès |

---

## Structure du Projet

```
aeroscope/
├── SRC/
│   ├── air_quality/                  # Module Python qualité de l'air
│   │   ├── __init__.py
│   │   ├── client.py                 # Client API OpenAQ v3
│   │   ├── cities.py                 # Définitions de villes européennes
│   │   ├── config.py                 # Configuration & variables d'env
│   │   ├── orchestrator.py           # Orchestration ingestion données
│   │   ├── open_meteo.py             # Client API Open-Meteo (prévisions)
│   │   ├── pipeline.py               # Transformation des données
│   │   ├── storage.py                # Opérations SQLite/base de données
│   │   └── visualization.py          # Génération graphiques matplotlib
│   │
│   ├── cloud_run_services/           # Microservices ETL déployés
│   │   ├── weatherapicom/
│   │   │   ├── main.py               # Collecteur WeatherAPI.com
│   │   │   ├── Dockerfile
│   │   │   └── requirements.txt
│   │   ├── openaq-collector/
│   │   │   ├── main.py               # Collecteur OpenWeatherMap AQI
│   │   │   └── requirements.txt      # (50 villes FR+EU)
│   │   ├── airqualitynew/
│   │   │   ├── main.py               # Historique Open-Meteo (24 mois)
│   │   │   └── requirements.txt
│   │   └── qualiteair/
│   │       ├── main.py               # Service qualité air alternatif
│   │       └── requirements.txt
│   │
│   └── web_app/                      # Application Next.js
│       ├── app/
│       │   ├── page.tsx              # Dashboard principal avec state
│       │   ├── layout.tsx            # Layout racine + metadata
│       │   ├── globals.css           # Styles Tailwind + animations
│       │   ├── api/
│       │   │   └── weather_data/
│       │   │       └── route.ts      # API agrégation multi-sources
│       │   ├── components/
│       │   │   ├── google_map_component.tsx
│       │   │   │   # Carte interactive + marqueurs
│       │   │   └── air_quality_chart_component.tsx
│       │   │       # Graphiques historiques Recharts
│       │   └── types/
│       │       └── index.ts          # Interfaces TypeScript
│       ├── package.json              # Dépendances Node.js
│       ├── tsconfig.json             # Config TypeScript strict
│       ├── next.config.ts            # Config Next.js standalone
│       └── tailwind.config.ts        # Config Tailwind CSS v4
│
├── main.py                           # Point d'entrée ETL local/WSGI
├── cloudbuild-trigger.yaml           # Trigger CI/CD GitHub→Cloud Run
├── README.md                         # Ce fichier
└── .gitignore
```

### Répertoires Clés

- **`SRC/air_quality/`** : Module Python réutilisable pour la qualité de l'air
  - **`client.py`** : Client OpenAQ API v3 avec gestion erreurs et sélection automatique de capteurs
  - **`cities.py`** : Définitions de villes européennes (coordonnées GPS, métadonnées pays)
  - **`config.py`** : Gestion configuration via variables d'environnement
  - **`orchestrator.py`** : Pipeline orchestration ingestion avec rapport succès/échec
  - **`open_meteo.py`** : Client Open-Meteo pour prévisions qualité air (PM2.5, O₃, NO₂)
  - **`pipeline.py`** : Transformations ETL (normalisation polluants, agrégation temporelle)
  - **`storage.py`** : Couche abstraction SQLite pour stockage local des mesures
  - **`visualization.py`** : Génération graphiques matplotlib (séries temporelles, comparaisons)

- **`SRC/cloud_run_services/`** : Microservices Flask déployés sur Google Cloud Run
  - **`weatherapicom/`** : Collecte données météo temps réel (3 villes test)
  - **`openaq-collector/`** : Collecte pollution AQI OpenWeatherMap (50 villes FR+EU)
  - **`airqualitynew/`** : Ingestion historique Open-Meteo (2024-2025, 3 pays)
  - **`qualiteair/`** : Service complémentaire qualité air
  - Chaque service est autonome : déploiement indépendant, scaling automatique

- **`SRC/web_app/`** : Application Next.js 16 avec App Router
  - **Architecture** : React 19, TypeScript strict, Tailwind CSS v4
  - **API Routes** : Agrégation multi-sources BigQuery (météo + pollution)
  - **Composants** : GoogleMapComponent (marqueurs interactifs), AirQualityChart (Recharts)
  - **Optimisations** : SSR, code splitting, caching stratégique

---

## Prérequis

### Requis

- **Node.js** : ≥ 20.9.0 (pour Next.js)
- **Python** : 3.11 (pour les scripts ETL)
- **npm/yarn/pnpm** : Gestionnaire de paquets
- **Compte Google Cloud** : Avec facturation activée
- **Google Cloud CLI** : Pour déploiement et authentification
- **Clés API** :
  - Clé API Google Maps
  - Clé API WeatherAPI.com
  - Clé API OpenWeatherMap

### Recommandé

- **Git** : Contrôle de version
- **Docker** : Tests de conteneurs (optionnel)
- **VS Code** : IDE avec extensions TypeScript/Python
- **Accès BigQuery** : Via Google Cloud Console

---

## Installation

### Configuration Backend (ETL Python)

#### 1. Cloner le Référentiel

```bash
git clone https://github.com/your-org/aeroscope.git
cd aeroscope
```

#### 2. Configurer l'Environnement Python

```bash
# Créer environnement virtuel
python3.11 -m venv venv
source venv/bin/activate  # Sur Windows : venv\Scripts\activate

# Installer le module air_quality (développement local)
pip install -e .  # Si pyproject.toml/setup.py existe
# OU installer les dépendances directement :
pip install requests pandas matplotlib google-cloud-bigquery --break-system-packages

# Installer les dépendances pour chaque microservice
cd SRC/cloud_run_services/weatherapicom
pip install -r requirements.txt --break-system-packages

cd ../openaq-collector
pip install -r requirements.txt --break-system-packages

cd ../airqualitynew
pip install -r requirements.txt --break-system-packages

# Répéter pour qualiteair...
```

**Note sur le module `air_quality`** : Ce module contient toute la logique métier pour :
- Communication avec APIs externes (OpenAQ v3, Open-Meteo)
- Pipeline ETL (transformation, validation)
- Stockage local SQLite (pour tests/dev)
- Visualisations matplotlib

#### 3. Configurer les Variables d'Environnement

Chaque service nécessite des variables d'environnement spécifiques :

**Module `air_quality` (local/dev) :**
```bash
export OPENAQ_API_KEY="votre_cle_openaq"  # Optionnel (niveau gratuit OK)
export DATABASE_PATH="./air_quality.db"   # Stockage SQLite local
export ARTIFACTS_DIR="./artifacts"        # Sortie visualisations
```

**Service `weatherapicom` :**
```bash
export WEATHER_API_KEY="votre_cle_weatherapi"
export PORT=8080
# Collecte : Paris, Berlin, Bruxelles (3 villes test)
```

**Service `openaq-collector` :**
```bash
export OPENWEATHERMAP_API_KEY="votre_cle_openweathermap"
export PORT=8080
# Collecte : 30 villes FR + 20 villes EU = 50 villes
# Mode historique disponible via ?history=true
```

**Service `airqualitynew` :**
```bash
export PORT=8080
# API Open-Meteo gratuite, pas de clé requise
# Collecte historique : France, Allemagne, Belgique (2024-2025)
```

**Service `qualiteair` :**
```bash
export PORT=8080
# Configuration API spécifique selon source de données
```

#### 4. Tests Locaux

**A. Tester le module air_quality en local :**

```bash
# Script principal avec visualisations
python main.py \
  --cities Paris Berlin London \
  --forecast-hours 72 \
  --log-level INFO

# Mode ingestion seule (sans graphiques)
python main.py --skip-visuals

# Avec villes personnalisées depuis JSON
echo '{"CustomCity": {"country": "FR", "latitude": 48.0, "longitude": 2.0}}' > cities.json
python main.py --city-overrides cities.json

# Vérifier que les visualisations sont générées dans artifacts/
ls -lh artifacts/*.png
```

**B. Tester les microservices individuellement :**

```bash
# Tester weatherapicom
cd SRC/cloud_run_services/weatherapicom
python main.py
# Dans un autre terminal :
curl http://localhost:8080/fetch_weather

# Tester openaq-collector
cd ../openaq-collector
python main.py
curl http://localhost:8080/

# Tester openaq-collector en mode historique (2 ans)
curl "http://localhost:8080/?history=true"
```

#### 5. Déployer sur Cloud Run

```bash
# S'authentifier
gcloud auth login
gcloud config set project VOTRE_PROJECT_ID

# Déployer le service
cd SRC/cloud_run_services/weatherapicom
gcloud run deploy weatherapicom \
  --source . \
  --region europe-west1 \
  --allow-unauthenticated \
  --set-env-vars WEATHER_API_KEY=$WEATHER_API_KEY

# Répéter pour les autres services...
```

#### 6. Configurer Cloud Scheduler (Optionnel)

```bash
# Créer un job pour déclencher l'ETL toutes les heures
gcloud scheduler jobs create http weatherapi-hourly \
  --schedule="0 * * * *" \
  --uri="https://weatherapicom-xxxxx-ew.a.run.app/fetch_weather" \
  --http-method=GET \
  --location=europe-west1
```

---

### Configuration Frontend (Next.js)

#### 1. Naviguer vers l'Application Web

```bash
cd SRC/web_app
```

#### 2. Installer les Dépendances

```bash
npm install
# ou
yarn install
# ou
pnpm install
```

#### 3. Configurer l'Environnement

Créer le fichier `.env.local` :

```bash
# .env.local

# Clé API Google Maps (requis)
NEXT_PUBLIC_GOOGLE_MAPS_API_KEY=votre_cle_api_google_maps

# ID du projet Google Cloud (auto-détecté si credentials configurés)
# GCP_PROJECT_ID=votre-project-id

# Optionnel
# NODE_ENV=development
```

#### 4. Configurer l'Authentification Google Cloud

**Option A : Développement Local**

```bash
# Installer gcloud CLI
curl https://sdk.cloud.google.com | bash

# S'authentifier
gcloud auth login
gcloud auth application-default login

# Définir le projet
gcloud config set project VOTRE_PROJECT_ID
```

**Option B : Production (Service Account)**

```bash
# Télécharger la clé du compte de service depuis GCP Console
# IAM & Admin > Service Accounts > Create Key

# Définir la variable d'environnement
export GOOGLE_APPLICATION_CREDENTIALS="/chemin/vers/key.json"
```

#### 5. Vérifier l'Accès BigQuery

Assurez-vous que ces datasets existent dans votre projet BigQuery :

```sql
-- Vérifier les datasets
SELECT * FROM `votre-projet.weather_data.INFORMATION_SCHEMA.TABLES`;
SELECT * FROM `votre-projet.airquality_full.INFORMATION_SCHEMA.TABLES`;
```

Tables requises :
- `weather_data.weather_records`
- `weather_data.weather_monthly_avg`
- `weather_data.weatherapicom`
- `airquality_full.measurements_complette`

#### 6. Démarrer le Serveur de Développement

```bash
npm run dev
```

Ouvrir [http://localhost:3000](http://localhost:3000)

---

## Configuration

### Schéma BigQuery

**weather_data.weather_records**
```sql
CREATE TABLE `weather_data.weather_records` (
  city STRING,
  country STRING,
  latitude FLOAT64,
  longitude FLOAT64,
  temperature FLOAT64,
  weather_description STRING,
  wind_speed FLOAT64,
  humidity INTEGER,
  clouds INTEGER,
  timestamp TIMESTAMP
)
PARTITION BY DATE(timestamp);
```

**weather_data.weather_monthly_avg**
```sql
CREATE TABLE `weather_data.weather_monthly_avg` (
  country STRING,
  year INTEGER,
  month STRING,
  temperature_2m FLOAT64,
  cloudcover FLOAT64,
  weather_description STRING
);
```

### Limites de Taux d'API

| Service | Limite de Taux | Villes Couvertes | Notes |
|---------|-----------|------------------|-------|
| WeatherAPI.com | 1M appels/mois | 3 villes test (Paris, Berlin, Bruxelles) | Niveau gratuit |
| OpenWeatherMap AQI | 1000 appels/jour | **50 villes** (30 FR + 20 EU) | Niveau gratuit, API pollution |
| Open-Meteo | Illimité | 3 pays (FR, DE, BE) | Gratuit, pas de clé, historique 24 mois |
| OpenAQ v3 | 10 000 appels/jour | Configurable via module `air_quality` | Niveau gratuit, capteurs temps réel |

**Détail des 50 villes OpenWeatherMap** :
- **30 villes françaises** : Paris, Marseille, Lyon, Toulouse, Nice, Nantes, Strasbourg, Montpellier, Bordeaux, Lille, Rennes, Reims, Le Havre, Saint-Étienne, Toulon, Grenoble, Dijon, Angers, Nîmes, Villeurbanne, Clermont-Ferrand, Aix-en-Provence, Brest, Limoges, Tours, Amiens, Perpignan, Metz, Besançon, Orléans
- **20 villes européennes** : London (GB), Berlin (DE), Madrid (ES), Rome (IT), Barcelona (ES), Vienna (AT), Hamburg (DE), Munich (DE), Milan (IT), Prague (CZ), Brussels (BE), Amsterdam (NL), Lisbon (PT), Athens (GR), Stockholm (SE), Warsaw (PL), Budapest (HU), Copenhagen (DK), Dublin (IE), Oslo (NO)

---

**Construit avec ❤️ pour la sensibilisation environnementale**