'use client';

import { useEffect, useState } from 'react';
import { ContextProvider } from '../components/google_map_context';
import GoogleMapComponent from '../components/google_map_component';

export default function Page() {
  const [stations, setStations] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Fetch data from your API endpoint
  useEffect(() => {
    async function fetchStations() {
      try {
        setLoading(true);
        const response = await fetch('/api/weather_data');
        
        if (!response.ok) {
          throw new Error('Failed to fetch weather data');
        }
        
        const data = await response.json();
        setStations(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Unknown error occurred');
        console.error('Error fetching stations:', err);
      } finally {
        setLoading(false);
      }
    }

    fetchStations();
  }, []);

  // Render loading state
  if (loading) {
    return (
      <div className="flex items-center justify-center h-screen">
        <div className="text-lg text-gray-600">Loading weather stations...</div>
      </div>
    );
  }

  // Render error state
  if (error) {
    return (
      <div className="flex items-center justify-center h-screen">
        <div className="text-lg text-red-600">Error: {error}</div>
      </div>
    );
  }

  return (
    <ContextProvider>
      <div className="flex flex-col h-screen">
        {/* Header */}
        <header className="bg-blue-600 text-white p-4">
          <h1 className="text-2xl font-bold">Europe Air Quality Map</h1>
          <p className="text-sm mt-1">Total stations: {stations.length}</p>
        </header>

        {/* Main content */}
        <main className="flex-1 flex gap-4 p-4">
          {/* Sidebar for filters (optional - can add FilterControls here) */}
          <aside className="w-64 bg-gray-100 p-4 rounded">
            <h2 className="text-lg font-bold mb-4">Filters</h2>
            {/* Import FilterControls component here later */}
            <p className="text-gray-600 text-sm">Filter controls coming soon...</p>
          </aside>

          {/* Map container */}
          <section className="flex-1">
            <GoogleMapComponent stations={stations} />
          </section>
        </main>
      </div>
    </ContextProvider>
  );
}
