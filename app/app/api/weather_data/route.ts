import { NextResponse } from 'next/server';
import { BigQuery } from '@google-cloud/bigquery';

export const dynamic = 'force-dynamic';

export async function GET() {
  const projectId = process.env.GOOGLE_CLOUD_PROJECT;

  if (!projectId) {
    return NextResponse.json({ error: "Projet ID manquant" }, { status: 500 });
  }

  try {
    // Client sans région par défaut pour gérer le multi-location
    const bigquery = new BigQuery({ projectId: projectId });

    // --- REQUÊTE 1 : HISTORIQUE POLLUTION (Pays) ---
    // Région : EU
    const queryPollution = `
      SELECT 
        country,
        month, -- Format String "YYYY-MM"
        AVG(pm2_5) as pm2_5,
        AVG(pm10) as pm10,
        AVG(nitrogen_dioxide) as nitrogen_dioxide,
        AVG(ozone) as ozone
      FROM \`${projectId}.air_quality_europe_monthly_avg.donnees_open_meteo_qualite_air\`
      -- On parse la date pour filtrer les 2 dernières années
      WHERE PARSE_DATE('%Y-%m', month) >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
      GROUP BY country, month
    `;

    // --- REQUÊTE 2 : HISTORIQUE MÉTÉO (Pays) ---
    // Région : europe-west9
    const queryWeatherHist = `
      SELECT 
        country,
        month,
        AVG(temperature_2m) as temperature_2m,
        AVG(cloudcover) as cloudcover,
        ANY_VALUE(weather_description_mode) as weather_description
      FROM \`${projectId}.weather_data.weather_monthly_avg\`
      WHERE PARSE_DATE('%Y-%m', month) >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
      GROUP BY country, month
    `;

    // --- REQUÊTE 3 : VILLES & LIVE (Villes) ---
    // Région : europe-west9
    const queryRealtime = `
      SELECT 
        city,
        country,
        lat as latitude,
        lon as longitude,
        temp_c as temperature_2m,
        condition as weather_description,
        wind_kph,
        humidity,
        cloud as cloudcover
      FROM \`${projectId}.weather_data.weatherapicom\`
      -- On prend la dernière donnée connue pour chaque ville
      QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY timestamp DESC) = 1
    `;

    // ⚡ Exécution Parallèle
    const [polRes, weatherRes, realtimeRes] = await Promise.all([
      bigquery.query(queryPollution),
      bigquery.query(queryWeatherHist),
      bigquery.query(queryRealtime)
    ]);

    const pollutionRows = polRes[0];
    const weatherHistRows = weatherRes[0];
    const realtimeRows = realtimeRes[0];

    // --- ASSEMBLAGE ---

    // 1. Dictionnaire Historique par Pays (Fusion Pollution + Météo Hist)
    const countryHistoryMap: Record<string, Record<string, any>> = {};

    // Remplissage Pollution
    pollutionRows.forEach((row: any) => {
      const c = row.country ? row.country.toLowerCase() : "unknown";
      if (!countryHistoryMap[c]) countryHistoryMap[c] = {};
      
      // On initialise l'objet mois s'il n'existe pas
      if (!countryHistoryMap[c][row.month]) {
        countryHistoryMap[c][row.month] = { month: row.month };
      }
      
      // On fusionne les données
      Object.assign(countryHistoryMap[c][row.month], {
        pm2_5: row.pm2_5,
        pm10: row.pm10,
        nitrogen_dioxide: row.nitrogen_dioxide,
        ozone: row.ozone
      });
    });

    // Remplissage Météo Historique
    weatherHistRows.forEach((row: any) => {
      const c = row.country ? row.country.toLowerCase() : "unknown";
      if (!countryHistoryMap[c]) countryHistoryMap[c] = {};
      if (!countryHistoryMap[c][row.month]) {
        countryHistoryMap[c][row.month] = { month: row.month };
      }
      
      Object.assign(countryHistoryMap[c][row.month], {
        temperature_2m: row.temperature_2m,
        cloudcover: row.cloudcover,
        weather_description: row.weather_description
      });
    });

    const finalData: any[] = [];
    const currentMonthPrefix = new Date().toISOString().slice(0, 7); // "2024-05"

    // 2. Génération des données par Ville
    realtimeRows.forEach((cityRow: any) => {
      const countryKey = cityRow.country ? cityRow.country.toLowerCase() : "";
      
      // On récupère tout l'historique disponible pour le pays de cette ville
      const historyMonths = countryHistoryMap[countryKey] 
        ? Object.values(countryHistoryMap[countryKey]) 
        : [];

      // Si on a de l'historique, on le traite
      if (historyMonths.length > 0) {
        // Tri du plus récent au plus ancien
        historyMonths.sort((a: any, b: any) => b.month.localeCompare(a.month));

        // On vérifie si le mois courant est présent
        let hasCurrentMonth = false;

        const cityHistory = historyMonths.map((histRow: any, index: number) => {
          const isMostRecent = index === 0; // Le premier après le tri est le plus récent
          const isCurrentMonth = histRow.month === currentMonthPrefix;
          
          if (isCurrentMonth) hasCurrentMonth = true;

          // Si c'est le mois courant OU le plus récent, on affiche la météo LIVE de la ville
          // (Priorité à la météo temps réel précise)
          if (isCurrentMonth || (isMostRecent && !hasCurrentMonth)) {
             return {
               ...cityRow, // Lat, Lon, Ville, Pays, Météo Live
               month: isCurrentMonth ? histRow.month : currentMonthPrefix,
               
               // Pollution (Moyenne Pays)
               pm2_5: histRow.pm2_5 || 0,
               pm10: histRow.pm10 || 0,
               nitrogen_dioxide: histRow.nitrogen_dioxide || 0,
               ozone: histRow.ozone || 0,
               
               is_realtime: true
             };
          } else {
             // Archive pure (Moyenne Pays Pollution + Moyenne Pays Météo)
             return {
               city: cityRow.city,
               country: cityRow.country,
               latitude: cityRow.latitude,
               longitude: cityRow.longitude,
               month: histRow.month,
               
               pm2_5: histRow.pm2_5 || 0,
               pm10: histRow.pm10 || 0,
               nitrogen_dioxide: histRow.nitrogen_dioxide || 0,
               ozone: histRow.ozone || 0,
               
               temperature_2m: histRow.temperature_2m || 0,
               cloudcover: histRow.cloudcover || 0,
               weather_description: histRow.weather_description || "Archive",
               
               wind_kph: 0, humidity: 0, is_realtime: false
             };
          }
        });
        
        finalData.push(...cityHistory);
        
      } else {
        // Cas de secours : Ville sans historique pays connu
        // On affiche au moins la météo actuelle
        finalData.push({
          ...cityRow,
          month: currentMonthPrefix,
          pm2_5: 0, pm10: 0, nitrogen_dioxide: 0, ozone: 0,
          is_realtime: true
        });
      }
    });

    return NextResponse.json(finalData, {
      headers: { 'Cache-Control': 'no-store, max-age=0' }
    });

  } catch (error: any) {
    console.error('ERREUR API:', error);
    return NextResponse.json(
      { error: error.message, details: "Erreur fusion finale" }, 
      { status: 500 }
    );
  }
}