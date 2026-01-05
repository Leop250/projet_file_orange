import { NextResponse } from 'next/server';
import { BigQuery } from '@google-cloud/bigquery';
import { GoogleAuth } from 'google-auth-library';

export const dynamic = 'force-dynamic';

export async function GET() {
  const auth = new GoogleAuth();
  const projectId = await auth.getProjectId();

  if (!projectId) {
    return NextResponse.json(
      { error: 'Projet ID manquant' },
      { status: 500 }
    );
  }

  try {
    const bigquery = new BigQuery({ projectId });

    // --- REQUÊTE 1 : HISTORIQUE MÉTÉO (Pays) ---
    const queryWeatherHist = `
      SELECT
        country,
        month,
        AVG(temperature_2m) AS temperature_2m,
        AVG(cloudcover) AS cloudcover,
        ANY_VALUE(weather_description_mode) AS weather_description
      FROM \`${projectId}.weather_data.weather_monthly_avg\`
      WHERE PARSE_DATE('%Y-%m', month) >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
      GROUP BY country, month
    `;

    // --- REQUÊTE 2 : MÉTÉO TEMPS RÉEL (Villes) ---
    const queryWeatherRealtime = `
      SELECT
        city_name AS city,
        country,
        latitude,
        longitude,
        temperature AS temperature_2m,
        weather_description,
        wind_speed AS wind_kph,
        humidity,
        clouds AS cloudcover,
        timestamp
      FROM \`${projectId}.weather_data.weather_records\`
      QUALIFY ROW_NUMBER() OVER (PARTITION BY city_name ORDER BY timestamp DESC) = 1
    `;

    // --- REQUÊTE 3 : POLLUTION (Historique + Temps réel, par ville) ---
    // 🔧 Correction ici : on compare sur DATE(measurement_timestamp)
    const queryPollutionFull = `
      SELECT
        city,
        country,
        latitude,
        longitude,
        FORMAT_TIMESTAMP('%Y-%m', measurement_timestamp) AS month,
        measurement_timestamp,
        pm2_5,
        pm10,
        no2,
        o3,
        so2,
        co,
        nh3,
        aqi
      FROM \`${projectId}.airquality_full.measurements_complette\`
      WHERE DATE(measurement_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
    `;

    // ⚡ Exécution en parallèle
    const [weatherHistRes, weatherRealtimeRes, pollutionFullRes] = await Promise.all([
      bigquery.query(queryWeatherHist),
      bigquery.query(queryWeatherRealtime),
      bigquery.query(queryPollutionFull),
    ]);

    const weatherHistRows = weatherHistRes[0];
    const weatherRealtimeRows = weatherRealtimeRes[0];
    const pollutionRows = pollutionFullRes[0];

    // --- 1. Historique météo par pays
    const countryWeatherHistMap: Record<string, Record<string, any>> = {};

    weatherHistRows.forEach((row: any) => {
      const c = row.country ? row.country.toLowerCase() : 'unknown';
      if (!countryWeatherHistMap[c]) countryWeatherHistMap[c] = {};
      if (!countryWeatherHistMap[c][row.month]) {
        countryWeatherHistMap[c][row.month] = { month: row.month };
      }

      Object.assign(countryWeatherHistMap[c][row.month], {
        temperature_2m: row.temperature_2m,
        cloudcover: row.cloudcover,
        weather_description: row.weather_description,
      });
    });

    // --- 2. Historique + temps réel pollution par ville ET pays
    const cityPollutionHistMap: Record<string, Record<string, any[]>> = {};
    const cityLatestPollutionMap: Record<string, any> = {};

    pollutionRows.forEach((row: any) => {
      const city = row.city ? row.city.toLowerCase() : 'unknown';
      const country = row.country ? row.country.toLowerCase() : 'unknown';
      const key = `${city}|${country}`;

      // Historique par ville
      if (!cityPollutionHistMap[key]) cityPollutionHistMap[key] = {};
      if (!cityPollutionHistMap[key][row.month]) cityPollutionHistMap[key][row.month] = [];
      cityPollutionHistMap[key][row.month].push(row);

      // Temps réel : on garde la mesure la plus récente
      const currentLatest = cityLatestPollutionMap[key];
      if (
        !currentLatest ||
        new Date(row.measurement_timestamp).getTime() >
          new Date(currentLatest.measurement_timestamp).getTime()
      ) {
        cityLatestPollutionMap[key] = row;
      }
    });

    // --- 3. Assemblage final
    const finalData: any[] = [];
    const currentMonthPrefix = new Date().toISOString().slice(0, 7);

    weatherRealtimeRows.forEach((weatherRow: any) => {
      const city = weatherRow.city ? weatherRow.city.toLowerCase() : '';
      const country = weatherRow.country ? weatherRow.country.toLowerCase() : '';
      const cityCountryKey = `${city}|${country}`;

      // Pollution temps réel pour cette ville
      const latestPoll = cityLatestPollutionMap[cityCountryKey];
      const realtimePoll = latestPoll
        ? {
            pm2_5: latestPoll.pm2_5 ?? 0,
            pm10: latestPoll.pm10 ?? 0,
            no2: latestPoll.no2 ?? 0,
            o3: latestPoll.o3 ?? 0,
            so2: latestPoll.so2 ?? 0,
            co: latestPoll.co ?? 0,
            nh3: latestPoll.nh3 ?? 0,
            aqi: latestPoll.aqi ?? null,
          }
        : {
            pm2_5: 0,
            pm10: 0,
            no2: 0,
            o3: 0,
            so2: 0,
            co: 0,
            nh3: 0,
            aqi: null,
          };

      // Donnée temps réel fusionnée
      finalData.push({
        city: weatherRow.city,
        country: weatherRow.country,
        latitude: weatherRow.latitude,
        longitude: weatherRow.longitude,
        month: currentMonthPrefix,
        temperature_2m: weatherRow.temperature_2m,
        cloudcover: weatherRow.cloudcover,
        weather_description: weatherRow.weather_description,
        wind_kph: weatherRow.wind_kph,
        humidity: weatherRow.humidity,
        ...realtimePoll,
        is_realtime: true,
      });

      // Historique météo par pays
      const countryHistMonths = countryWeatherHistMap[country]
        ? Object.values(countryWeatherHistMap[country])
        : [];

      // Historique pollution par ville
      const cityPollByMonth = cityPollutionHistMap[cityCountryKey] || {};

      // Génération des archives
      countryHistMonths.forEach((histWeather: any) => {
        const month = histWeather.month;
        if (month === currentMonthPrefix) return;

        const cityMonthPollArray = cityPollByMonth[month] || [];

        let aggPoll = {
          pm2_5: 0,
          pm10: 0,
          no2: 0,
          o3: 0,
          so2: 0,
          co: 0,
          nh3: 0,
          aqi: null as number | null,
        };

        if (cityMonthPollArray.length > 0) {
          const n = cityMonthPollArray.length;
          const sum = cityMonthPollArray.reduce(
            (acc: any, r: any) => {
              acc.pm2_5 += r.pm2_5 ?? 0;
              acc.pm10 += r.pm10 ?? 0;
              acc.no2 += r.no2 ?? 0;
              acc.o3 += r.o3 ?? 0;
              acc.so2 += r.so2 ?? 0;
              acc.co += r.co ?? 0;
              acc.nh3 += r.nh3 ?? 0;
              if (r.aqi != null) {
                acc.aqiSum += r.aqi;
                acc.aqiCount += 1;
              }
              return acc;
            },
            {
              pm2_5: 0,
              pm10: 0,
              no2: 0,
              o3: 0,
              so2: 0,
              co: 0,
              nh3: 0,
              aqiSum: 0,
              aqiCount: 0,
            }
          );

          aggPoll = {
            pm2_5: sum.pm2_5 / n,
            pm10: sum.pm10 / n,
            no2: sum.no2 / n,
            o3: sum.o3 / n,
            so2: sum.so2 / n,
            co: sum.co / n,
            nh3: sum.nh3 / n,
            aqi: sum.aqiCount > 0 ? Math.round(sum.aqiSum / sum.aqiCount) : null,
          };
        }

        finalData.push({
          city: weatherRow.city,
          country: weatherRow.country,
          latitude: weatherRow.latitude,
          longitude: weatherRow.longitude,
          month,
          temperature_2m: histWeather.temperature_2m ?? 0,
          cloudcover: histWeather.cloudcover ?? 0,
          weather_description: histWeather.weather_description ?? 'Archive',
          wind_kph: 0,
          humidity: 0,
          ...aggPoll,
          is_realtime: false,
        });
      });
    });

    return NextResponse.json(finalData, {
      headers: { 'Cache-Control': 'no-store, max-age=0' },
    });
  } catch (error: any) {
    console.error('ERREUR API:', error);
    return NextResponse.json(
      { error: error.message, details: 'Erreur fusion finale' },
      { status: 500 }
    );
  }
}
