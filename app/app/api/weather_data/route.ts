import { NextResponse } from 'next/server';
import { BigQuery } from '@google-cloud/bigquery';

// Initialisation du client (authentification automatique sur Cloud Run)
const bigquery = new BigQuery();

export async function GET() {
  try {
    const projectId = process.env.GOOGLE_CLOUD_PROJECT;
    
    // La requête SQL magique
    // QUALIFY ... = 1 permet de ne garder que la ligne la plus récente pour chaque ville
    const query = `
      SELECT 
        city,
        country,
        lat, 
        lon,
        temp_c,
        condition,
        humidity,
        wind_kph,
        FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S', timestamp) as last_update
      FROM \`${projectId}.weather_data.weatherapicom\`
      WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
      QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY timestamp DESC) = 1
    `;

    // Exécution de la requête
    const [rows] = await bigquery.query(query);

    return NextResponse.json(rows, {
      headers: {
        'Cache-Control': 'no-store, max-age=0', // Pas de cache, on veut du temps réel
        'Content-Type': 'application/json'
      }
    });

  } catch (error) {
    console.error('Erreur BigQuery:', error);
    return NextResponse.json(
      { error: 'Erreur lors de la récupération des données météo' }, 
      { status: 500 }
    );
  }
}