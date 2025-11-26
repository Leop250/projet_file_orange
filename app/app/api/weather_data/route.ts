import { NextRequest, NextResponse } from 'next/server';
import fs from 'fs';
import path from 'path';
import csv from 'csv-parser';

export async function GET(request: NextRequest) {
  try {
    const csvFilePath = path.join(process.cwd(), 'public', 'air_quality_europe.csv');
    
    // Check if file exists
    if (!fs.existsSync(csvFilePath)) {
      return NextResponse.json(
        { error: 'CSV file not found' },
        { status: 404 }
      );
    }

    const results: any[] = [];

    return new Promise((resolve) => {
      fs.createReadStream(csvFilePath)
        .pipe(csv())
        .on('data', (data) => {
          results.push(data);
        })
        .on('end', () => {
          // Optional: Limit results for testing or performance
          // const limitedResults = results.slice(0, 100);
          
          resolve(
            NextResponse.json(results, {
              headers: {
                'Content-Type': 'application/json',
                'Cache-Control': 'max-age=3600' // Cache for 1 hour
              }
            })
          );
        })
        .on('error', (error) => {
          resolve(
            NextResponse.json(
              { error: 'Failed to parse CSV', details: error.message },
              { status: 500 }
            )
          );
        });
    });
  } catch (error) {
    return NextResponse.json(
      { error: 'Internal server error', details: error instanceof Error ? error.message : 'Unknown error' },
      { status: 500 }
    );
  }
}
