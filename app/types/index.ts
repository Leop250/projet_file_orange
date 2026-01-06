// =============================================================================
// TYPES PRINCIPAUX - AIR QUALITY DATA
// =============================================================================

/**
 * Données de qualité de l'air et météo pour une ville à un moment donné
 * Utilisé par l'API et tous les composants frontend
 */
export interface AirQualityData {
  // --- Identification ---
  city: string;
  country: string;
  latitude: number;
  longitude: number;
  month: string; // Format "YYYY-MM"

  // --- Polluants (µg/m³) ---
  pm2_5: number;      // Particules fines < 2.5µm
  pm10: number;       // Particules < 10µm
  no2: number;        // Dioxyde d'azote
  o3: number;         // Ozone
  so2: number;        // Dioxyde de soufre
  co: number;         // Monoxyde de carbone
  nh3: number;        // Ammoniac
  aqi: number | null; // Indice de qualité de l'air (1-5)

  // --- Météo ---
  temperature_2m: number;      // Température (°C)
  cloudcover: number;          // Couverture nuageuse (%)
  weather_description: string; // Description textuelle

  // --- Optionnels (temps réel uniquement) ---
  wind_kph?: number;     // Vitesse du vent (km/h)
  humidity?: number;     // Humidité (%)
  is_realtime?: boolean; // true = données live, false = archive
}

// =============================================================================
// TYPES API - BIGQUERY ROWS
// =============================================================================

/**
 * Row BigQuery : Historique météo par pays
 * Note: Le champ `month` est reconstruit en "YYYY-MM" via SQL
 * (la table source a `year` INTEGER et `month` STRING séparés)
 */
export interface WeatherHistoryRow {
  country: string;
  month: string; // Format "YYYY-MM" (reconstruit via CONCAT dans la requête)
  temperature_2m: number | null;
  cloudcover: number | null;
  weather_description: string | null;
}

/**
 * Row BigQuery : Météo temps réel par ville
 * Note: Mappings dans la requête SQL:
 * - city_name → city
 * - temperature → temperature_2m
 * - wind_speed → wind_kph
 * - clouds → cloudcover
 */
export interface WeatherRealtimeRow {
  city: string;
  country: string;
  latitude: number;
  longitude: number;
  temperature_2m: number | null;
  weather_description: string | null;
  wind_kph: number | null;
  humidity: number | null;
  cloudcover: number | null;
  timestamp: string;
}

/**
 * Row BigQuery : Données de pollution
 */
export interface PollutionRow {
  city: string;
  country: string;
  latitude: number;
  longitude: number;
  month: string;
  measurement_timestamp: string;
  pm2_5: number | null;
  pm10: number | null;
  no2: number | null;
  o3: number | null;
  so2: number | null;
  co: number | null;
  nh3: number | null;
  aqi: number | null;
}

/**
 * Données de pollution agrégées
 */
export interface PollutionData {
  pm2_5: number;
  pm10: number;
  no2: number;
  o3: number;
  so2: number;
  co: number;
  nh3: number;
  aqi: number | null;
}

// =============================================================================
// TYPES UTILITAIRES
// =============================================================================

/**
 * Données groupées par ville
 */
export type GroupedAirQualityData = Record<string, AirQualityData[]>;

/**
 * Type pour les clés de polluants
 */
export type PollutantKey = 'pm2_5' | 'pm10' | 'no2' | 'o3' | 'so2' | 'co' | 'nh3';

/**
 * Réponse d'erreur API
 */
export interface ApiErrorResponse {
  error: string;
  message?: string;
  details?: string;
}

// =============================================================================
// CONSTANTES
// =============================================================================

/**
 * Seuils de pollution PM2.5 (OMS)
 */
export const PM25_THRESHOLDS = {
  GOOD: 10,       // Bon
  MODERATE: 25,   // Modéré
  UNHEALTHY: 50,  // Mauvais pour groupes sensibles
  DANGEROUS: 75,  // Dangereux
} as const;

/**
 * Seuils AQI
 */
export const AQI_LEVELS: Record<number, string> = {
  1: 'Bon',
  2: 'Acceptable',
  3: 'Modéré',
  4: 'Mauvais',
  5: 'Très mauvais',
};

/**
 * Liste des clés de polluants (pour itération)
 */
export const POLLUTANT_KEYS: PollutantKey[] = [
  'pm2_5', 'pm10', 'no2', 'o3', 'so2', 'co', 'nh3'
];

/**
 * Configuration d'affichage des polluants
 */
export const POLLUTANT_CONFIG: Record<PollutantKey, { label: string; unit: string; color: string }> = {
  pm2_5: { label: 'PM2.5', unit: 'µg/m³', color: '#ef4444' },
  pm10:  { label: 'PM10',  unit: 'µg/m³', color: '#f97316' },
  no2:   { label: 'NO₂',   unit: 'µg/m³', color: '#eab308' },
  o3:    { label: 'O₃',    unit: 'µg/m³', color: '#22c55e' },
  so2:   { label: 'SO₂',   unit: 'µg/m³', color: '#3b82f6' },
  co:    { label: 'CO',    unit: 'mg/m³', color: '#8b5cf6' },
  nh3:   { label: 'NH₃',   unit: 'µg/m³', color: '#ec4899' },
};

/**
 * Valeurs par défaut pour les données de pollution
 */
export const DEFAULT_POLLUTION: PollutionData = {
  pm2_5: 0,
  pm10: 0,
  no2: 0,
  o3: 0,
  so2: 0,
  co: 0,
  nh3: 0,
  aqi: null,
};