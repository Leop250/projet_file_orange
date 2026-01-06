import { NextResponse } from 'next/server';
import { BigQuery } from '@google-cloud/bigquery';
import { GoogleAuth } from 'google-auth-library';
import {
  AirQualityData,
  PollutionData,
  DEFAULT_POLLUTION,
} from '@/types';

export const dynamic = 'force-dynamic';

// =============================================================================
// TYPES INTERNES (spécifiques aux requêtes BigQuery)
// =============================================================================

interface WeatherRealtimeRow {
  city: string;
  country: string;
  latitude: number;
  longitude: number;
  temperature: number | null;
  weather_description: string | null;
  wind_speed: number | null;
  humidity: number | null;
  clouds: number | null;
}

interface WeatherHistoryRow {
  country: string;
  year_month: string;
  temperature_2m: number | null;
  cloudcover: number | null;
  weather_description: string | null;
}

interface PollutionRow {
  city: string;
  country: string;
  latitude: number;
  longitude: number;
  year_month: string;
  pm2_5: number | null;
  pm10: number | null;
  no2: number | null;
  o3: number | null;
  so2: number | null;
  co: number | null;
  nh3: number | null;
  aqi: number | null;
}

interface CityInfo {
  city: string;
  country: string;
  latitude: number;
  longitude: number;
}

// =============================================================================
// CONSTANTES
// =============================================================================

// Mapping codes ISO → noms complets (pour matcher weather_monthly_avg)
const COUNTRY_CODE_TO_NAME: Record<string, string> = {
  'al': 'albania', 'ad': 'andorra', 'at': 'austria', 'be': 'belgium',
  'ba': 'bosnia and herzegovina', 'bg': 'bulgaria', 'hr': 'croatia',
  'cy': 'cyprus', 'cz': 'czech republic', 'dk': 'denmark', 'ee': 'estonia',
  'fi': 'finland', 'fr': 'france', 'de': 'germany', 'gr': 'greece',
  'hu': 'hungary', 'is': 'iceland', 'ie': 'ireland', 'it': 'italy',
  'lv': 'latvia', 'li': 'liechtenstein', 'lt': 'lithuania', 'lu': 'luxembourg',
  'mt': 'malta', 'md': 'moldova', 'mc': 'monaco', 'me': 'montenegro',
  'nl': 'netherlands', 'mk': 'north macedonia', 'no': 'norway', 'pl': 'poland',
  'pt': 'portugal', 'ro': 'romania', 'ru': 'russia', 'sm': 'san marino',
  'rs': 'serbia', 'sk': 'slovakia', 'si': 'slovenia', 'es': 'spain',
  'se': 'sweden', 'ch': 'switzerland', 'ua': 'ukraine',
  'gb': 'united kingdom', 'uk': 'united kingdom', 'va': 'vatican city',
};

// =============================================================================
// HELPERS
// =============================================================================

const normalize = (str: string | null | undefined): string =>
  (str ?? 'unknown').toLowerCase().trim();

const normalizeCountryForWeather = (country: string): string => {
  const normalized = normalize(country);
  return COUNTRY_CODE_TO_NAME[normalized] || normalized;
};

const getCityKey = (city: string, country: string): string =>
  `${normalize(city)}|${normalize(country)}`;

const extractPollution = (row: PollutionRow | null): PollutionData => {
  if (!row) return { ...DEFAULT_POLLUTION };
  return {
    pm2_5: row.pm2_5 ?? 0,
    pm10: row.pm10 ?? 0,
    no2: row.no2 ?? 0,
    o3: row.o3 ?? 0,
    so2: row.so2 ?? 0,
    co: row.co ?? 0,
    nh3: row.nh3 ?? 0,
    aqi: row.aqi ?? null,
  };
};

// =============================================================================
// QUERIES
// =============================================================================

const buildQueries = (projectId: string) => ({
  // 1. Météo temps réel par ville
  weatherRealtime: `
    SELECT
      city_name AS city,
      country,
      latitude,
      longitude,
      temperature,
      weather_description,
      wind_speed,
      humidity,
      clouds
    FROM \`${projectId}.weather_data.weather_records\`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city_name ORDER BY timestamp DESC) = 1
  `,

  // 2. Historique météo par pays (SOURCE PRINCIPALE pour l'historique)
  weatherHistory: `
    SELECT
      country,
      month AS year_month,
      AVG(temperature_2m) AS temperature_2m,
      AVG(cloudcover) AS cloudcover,
      ANY_VALUE(weather_description_mode) AS weather_description
    FROM \`${projectId}.weather_data.weather_monthly_avg\`
    GROUP BY country, month
    ORDER BY country, month DESC
  `,

  // 3. Pollution agrégée par ville/mois (pour enrichir quand disponible)
  pollutionHistory: `
    SELECT
      city,
      country,
      ANY_VALUE(latitude) AS latitude,
      ANY_VALUE(longitude) AS longitude,
      FORMAT_TIMESTAMP('%Y-%m', measurement_timestamp) AS year_month,
      AVG(pm2_5) AS pm2_5,
      AVG(pm10) AS pm10,
      AVG(no2) AS no2,
      AVG(o3) AS o3,
      AVG(so2) AS so2,
      AVG(co) AS co,
      AVG(nh3) AS nh3,
      CAST(ROUND(AVG(aqi)) AS INT64) AS aqi
    FROM \`${projectId}.airquality_full.measurements_complette\`
    GROUP BY city, country, FORMAT_TIMESTAMP('%Y-%m', measurement_timestamp)
  `,

  // 4. Pollution temps réel (dernière mesure par ville)
  pollutionLatest: `
    SELECT
      city,
      country,
      latitude,
      longitude,
      pm2_5,
      pm10,
      no2,
      o3,
      so2,
      co,
      nh3,
      aqi
    FROM \`${projectId}.airquality_full.measurements_complette\`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY measurement_timestamp DESC) = 1
  `,
});

// =============================================================================
// DATA ASSEMBLY
// =============================================================================

const assembleData = (
  weatherRealtime: WeatherRealtimeRow[],
  weatherHistory: WeatherHistoryRow[],
  pollutionHistory: PollutionRow[],
  pollutionLatest: PollutionRow[]
): AirQualityData[] => {
  const finalData: AirQualityData[] = [];
  const currentMonth = new Date().toISOString().slice(0, 7);

  // Index météo historique par pays (normalisé)
  const weatherHistByCountry: Record<string, Record<string, WeatherHistoryRow>> = {};
  weatherHistory.forEach(row => {
    const countryKey = normalize(row.country);
    if (!weatherHistByCountry[countryKey]) weatherHistByCountry[countryKey] = {};
    weatherHistByCountry[countryKey][row.year_month] = row;
  });

  // Index pollution par ville+mois
  const pollutionByCity: Record<string, Record<string, PollutionRow>> = {};
  pollutionHistory.forEach(row => {
    const cityKey = getCityKey(row.city, row.country);
    if (!pollutionByCity[cityKey]) pollutionByCity[cityKey] = {};
    pollutionByCity[cityKey][row.year_month] = row;
  });

  // Index pollution temps réel par ville
  const pollutionLatestByCity: Record<string, PollutionRow> = {};
  pollutionLatest.forEach(row => {
    pollutionLatestByCity[getCityKey(row.city, row.country)] = row;
  });

  // Index météo temps réel par ville
  const weatherRealtimeByCity: Record<string, WeatherRealtimeRow> = {};
  weatherRealtime.forEach(row => {
    weatherRealtimeByCity[getCityKey(row.city, row.country)] = row;
  });

  // Collecter toutes les villes uniques
  const allCities = new Map<string, CityInfo>();

  pollutionLatest.forEach(row => {
    const key = getCityKey(row.city, row.country);
    if (!allCities.has(key)) {
      allCities.set(key, {
        city: row.city,
        country: row.country,
        latitude: row.latitude,
        longitude: row.longitude,
      });
    }
  });

  weatherRealtime.forEach(row => {
    const key = getCityKey(row.city, row.country);
    if (!allCities.has(key)) {
      allCities.set(key, {
        city: row.city,
        country: row.country,
        latitude: row.latitude,
        longitude: row.longitude,
      });
    }
  });

  console.log(`Cities found: ${allCities.size}`);
  console.log(`Weather history countries: ${Object.keys(weatherHistByCountry).length}`);

  // Pour chaque ville, générer temps réel + historique
  allCities.forEach((cityInfo, cityKey) => {
    const { city, country, latitude, longitude } = cityInfo;
    const countryKeyForWeather = normalizeCountryForWeather(country);

    const weatherRT = weatherRealtimeByCity[cityKey];
    const pollutionRT = pollutionLatestByCity[cityKey];
    const cityPollutionHist = pollutionByCity[cityKey] || {};
    const countryWeatherHist = weatherHistByCountry[countryKeyForWeather] || {};

    // 1. TEMPS RÉEL
    finalData.push({
      city,
      country,
      latitude,
      longitude,
      month: currentMonth,
      temperature_2m: weatherRT?.temperature ?? 0,
      cloudcover: weatherRT?.clouds ?? 0,
      weather_description: weatherRT?.weather_description ?? 'N/A',
      wind_kph: weatherRT?.wind_speed ?? 0,
      humidity: weatherRT?.humidity ?? 0,
      ...extractPollution(pollutionRT || null),
      is_realtime: true,
    });

    // 2. HISTORIQUE (basé sur weather_monthly_avg du pays)
    Object.entries(countryWeatherHist).forEach(([yearMonth, weatherHist]) => {
      if (yearMonth === currentMonth) return;

      const pollutionForMonth = cityPollutionHist[yearMonth] || null;

      finalData.push({
        city,
        country,
        latitude,
        longitude,
        month: yearMonth,
        temperature_2m: weatherHist.temperature_2m ?? 0,
        cloudcover: weatherHist.cloudcover ?? 0,
        weather_description: weatherHist.weather_description ?? 'Archive',
        wind_kph: 0,
        humidity: 0,
        ...extractPollution(pollutionForMonth),
        is_realtime: false,
      });
    });
  });

  // Tri: par ville, puis temps réel en premier, puis par mois décroissant
  return finalData.sort((a, b) => {
    if (a.city !== b.city) return a.city.localeCompare(b.city);
    if (a.is_realtime && !b.is_realtime) return -1;
    if (!a.is_realtime && b.is_realtime) return 1;
    return b.month.localeCompare(a.month);
  });
};

// =============================================================================
// MAIN HANDLER
// =============================================================================

export async function GET() {
  try {
    const auth = new GoogleAuth();
    const projectId = await auth.getProjectId();

    if (!projectId) {
      return NextResponse.json(
        { error: 'Configuration error', message: 'Project ID not found' },
        { status: 500 }
      );
    }

    const bigquery = new BigQuery({ projectId });
    const queries = buildQueries(projectId);

    console.log('Executing BigQuery queries...');

    const [weatherRealtimeRes, weatherHistoryRes, pollutionHistoryRes, pollutionLatestRes] =
      await Promise.all([
        bigquery.query(queries.weatherRealtime),
        bigquery.query(queries.weatherHistory),
        bigquery.query(queries.pollutionHistory),
        bigquery.query(queries.pollutionLatest),
      ]);

    const weatherRealtimeRows = weatherRealtimeRes[0] as WeatherRealtimeRow[];
    const weatherHistoryRows = weatherHistoryRes[0] as WeatherHistoryRow[];
    const pollutionHistoryRows = pollutionHistoryRes[0] as PollutionRow[];
    const pollutionLatestRows = pollutionLatestRes[0] as PollutionRow[];

    console.log(`Data fetched:`);
    console.log(`  - Weather realtime: ${weatherRealtimeRows.length} cities`);
    console.log(`  - Weather history: ${weatherHistoryRows.length} records`);
    console.log(`  - Pollution history: ${pollutionHistoryRows.length} records`);
    console.log(`  - Pollution latest: ${pollutionLatestRows.length} cities`);

    if (weatherHistoryRows.length > 0) {
      const months = [...new Set(weatherHistoryRows.map(r => r.year_month))].sort().reverse();
      console.log(`Weather history months available: ${months.slice(0, 6).join(', ')}...`);
    }

    if (pollutionLatestRows.length === 0 && weatherRealtimeRows.length === 0) {
      return NextResponse.json(
        { error: 'No data', message: 'No data available' },
        { status: 404 }
      );
    }

    const finalData = assembleData(
      weatherRealtimeRows,
      weatherHistoryRows,
      pollutionHistoryRows,
      pollutionLatestRows
    );

    console.log(`Final data: ${finalData.length} records`);
    console.log(`  - Realtime: ${finalData.filter(d => d.is_realtime).length}`);
    console.log(`  - History: ${finalData.filter(d => !d.is_realtime).length}`);

    return NextResponse.json(finalData, {
      headers: {
        'Cache-Control': 'no-store, max-age=0',
        'X-Total-Count': String(finalData.length),
      },
    });
  } catch (error) {
    console.error('API Error:', error);

    if (error instanceof Error) {
      return NextResponse.json(
        {
          error: 'Server error',
          message: process.env.NODE_ENV === 'development' ? error.message : 'An error occurred',
        },
        { status: 500 }
      );
    }

    return NextResponse.json({ error: 'Unknown error' }, { status: 500 });
  }
}