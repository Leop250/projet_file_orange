import { NextResponse } from 'next/server';
import { Storage } from '@google-cloud/storage';
import csv from 'csv-parser';
import { WeatherStation } from '@/types';

const storage = new Storage();
const bucketName = 'projet-orange-bucket';
const fileName = 'mesures_actuelles.csv';

export async function GET() {
  const results: WeatherStation[] = [];

  return new Promise((resolve) => {
    const file = storage.bucket(bucketName).file(fileName);

    // Vérifie si le fichier existe avant de lire
    file.exists().then(([exists]) => {
      if (!exists) {
        resolve(NextResponse.json({ error: 'Fichier de données introuvable' }, { status: 404 }));
        return;
      }

      // Création du stream de lecture depuis le Cloud
      file.createReadStream()
        .pipe(csv())
        .on('data', (data: any) => {
            // Nettoyage éventuel et ajout au tableau
            results.push(data as WeatherStation);
        })
        .on('end', () => {
          resolve(NextResponse.json(results, {
            headers: {
              'Cache-Control': 'no-store, max-age=0', // ne pas mettre en cache pour avoir les maj
              'Content-Type': 'application/json'
            }
          }));
        })
        .on('error', (error) => {
          console.error('Erreur lecture GCS:', error);
          resolve(NextResponse.json({ error: 'Erreur lecture données' }, { status: 500 }));
        });
    });
  });
}