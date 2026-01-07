# AeroScope - Dashboard de Monitoring Environnemental

[![Next.js](https://img.shields.io/badge/Next.js-16.0-black?style=flat&logo=next.js)](https://nextjs.org/)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.x-blue?style=flat&logo=typescript)](https://www.typescriptlang.org/)
[![Google Cloud](https://img.shields.io/badge/Google%20Cloud-BigQuery-4285F4?style=flat&logo=google-cloud)](https://cloud.google.com/bigquery)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Plateforme web de visualisation temps réel des données de qualité de l'air et météorologiques pour 100+ villes dans le monde.

---

## Table des Matières

- [Vue d'Ensemble](#vue-densemble)
- [Architecture](#architecture)
- [Technologies](#technologies)
- [Prérequis](#prérequis)
- [Installation](#installation)

---

## Vue d'Ensemble

**AeroScope** est une application web full-stack moderne permettant de monitorer en temps réel la qualité de l'air et les conditions météorologiques de plus de 100 villes à travers le monde. Le projet combine des données de pollution atmosphérique (PM2.5, PM10, NO₂, O₃, SO₂, CO, NH₃) avec des informations météorologiques (température, humidité, vent) pour offrir une vision complète de l'environnement urbain.

### Objectifs

- **Sensibilisation** : Informer le public sur la qualité de l'air en temps réel
- **Analyse historique** : Fournir 24 mois d'historique pour identifier les tendances
- **Accessibilité** : Interface intuitive basée sur Google Maps
- **Performance** : Temps de chargement < 3 secondes, architecture scalable

### Cas d'Usage

- **Citoyens** : Consulter la qualité de l'air avant une sortie en extérieur
- **Chercheurs** : Analyser les tendances de pollution sur 2 ans
- **Décideurs publics** : Évaluer l'efficacité des politiques environnementales
- **Organisations** : Intégrer les données via API (future évolution)

---

## Architecture

### Vue d'Ensemble

```
┌─────────────────────────────────────────────────────────────┐
│                   SOURCES DE DONNÉES                        │
│  • API Météo (OpenWeatherMap, etc.)                         │
│  • API Qualité de l'Air (OpenAQ, etc.)                      │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                 PIPELINE ETL (scripts Python)               │
│  Extraction → Transformation → Chargement                   │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│              GOOGLE BIGQUERY (Data Warehouse)               │
│  • weather_data.weather_records (temps réel)                │
│  • weather_data.weather_monthly_avg (historique pays)       │
│  • airquality_full.measurements_complette (pollution)       │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│           APPLICATION WEB (Next.js + React)                 │
│                                                             │
│  ┌──────────────────────────────────────────────┐          │
│  │  API Routes (/api/weather_data)              │          │
│  │  • Authentification Google Cloud             │          │
│  │  • Requêtes BigQuery optimisées              │          │
│  │  • Fusion des sources de données             │          │
│  └────────────────┬─────────────────────────────┘          │
│                   │                                         │
│  ┌────────────────▼─────────────────────────────┐          │
│  │  Frontend (React + TypeScript)               │          │
│  │  • GoogleMapComponent (carte)                │          │
│  │  • AirQualityChart (graphiques)              │          │
│  │  • DashboardPage (orchestration)             │          │
│  └──────────────────────────────────────────────┘          │
└─────────────────────────────────────────────────────────────┘
```

### Flux de Données

1. **Ingestion** : Scripts ETL collectent les données des APIs externes
2. **Stockage** : Données structurées dans BigQuery (partitionnement par date)
3. **Requêtage** : API Next.js interroge BigQuery avec requêtes optimisées
4. **Transformation** : Fusion météo + pollution côté serveur
5. **Affichage** : Composants React rendent les données de manière interactive

### Architecture des Composants

```
app/
├── layout.tsx                    # Layout global + metadata
├── page.tsx                      # Page principale (orchestration)
├── globals.css                   # Styles globaux + animations
│
├── api/
│   └── weather_data/
│       └── route.ts             # Endpoint unique de données
│
├── components/
│   ├── google_map_component.tsx  # Carte Google Maps
│   └── air_quality_chart_component.tsx # Graphiques historiques
│
└── types/
    └── index.ts                  # Interfaces TypeScript
```

---

## Technologies

### Frontend

| Technologie | Version | Rôle |
|------------|---------|------|
| [Next.js](https://nextjs.org/) | 16.0 | Framework React avec SSR/SSG |
| [React](https://react.dev/) | 19.x | Bibliothèque UI componentisée |
| [TypeScript](https://www.typescriptlang.org/) | 5.x | Typage statique et sécurité |
| [Tailwind CSS](https://tailwindcss.com/) | 4.x | Framework CSS utility-first |
| [Recharts](https://recharts.org/) | 3.6+ | Graphiques SVG réactifs |
| [Google Maps API](https://developers.google.com/maps) | - | Cartographie interactive |

### Backend & Infrastructure

| Technologie | Version | Rôle |
|------------|---------|------|
| [Google BigQuery](https://cloud.google.com/bigquery) | - | Data warehouse analytique |
| [Google Cloud Auth](https://cloud.google.com/docs/authentication) | - | Authentification API |
| [Node.js](https://nodejs.org/) | 20+ | Runtime JavaScript serveur |
| API Routes Next.js | - | Endpoints serverless |

---

## Prérequis

### Obligatoires

- **Node.js** : Version 20.x ou supérieure
- **npm** / **yarn** / **pnpm** : Gestionnaire de paquets
- **Compte Google Cloud** : Pour BigQuery
- **Clé API Google Maps** : Pour la cartographie

### Recommandés

- **Git** : Pour cloner le repository
- **Docker** : Pour déploiement conteneurisé (optionnel)
- **VSCode** : IDE avec extensions TypeScript/React

---

## Installation

### 1. Cloner le Repository

```bash
git clone https://github.com/votre-organisation/aeroscope.git
cd aeroscope/app
```

### 2. Installer les Dépendances

```bash
npm install
# ou
yarn install
# ou
pnpm install
```

### 3. Configurer l'Environnement

Créez un fichier `.env.local` à la racine du dossier `app/` :

```bash
# .env.local

# Google Maps API Key (obligatoire)
NEXT_PUBLIC_GOOGLE_MAPS_API_KEY=votre_clé_api_maps

# Google Cloud Project ID (détecté automatiquement si credentials configurés)
# GCP_PROJECT_ID=votre-project-id

# Variables optionnelles
# NODE_ENV=development
```

### 4. Configurer Google Cloud

#### Option A : Authentification locale (développement)

```bash
# Installer gcloud CLI
curl https://sdk.cloud.google.com | bash

# S'authentifier
gcloud auth login
gcloud auth application-default login

# Définir le projet
gcloud config set project VOTRE_PROJECT_ID
```

#### Option B : Service Account (production)

```bash
# Télécharger la clé JSON depuis Google Cloud Console
# IAM & Admin > Service Accounts > Create Key

# Définir la variable d'environnement
export GOOGLE_APPLICATION_CREDENTIALS="/chemin/vers/key.json"
```

### 5. Vérifier la Configuration BigQuery

Assurez-vous que les datasets suivants existent dans BigQuery :

- `weather_data.weather_records`
- `weather_data.weather_monthly_avg`
- `airquality_full.measurements_complette`

### 6. Lancer le Serveur de Développement

```bash
npm run dev
```

Ouvrez [http://localhost:3000](http://localhost:3000) dans votre navigateur.

---


</div>