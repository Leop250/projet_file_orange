'use client';

import { useContext, useEffect, useState } from 'react';
import { GoogleMap, LoadScript, Marker, InfoWindow } from '@react-google-maps/api';
import { WeatherContext } from './googleMapContext';

const containerStyle = {
  width: '100%',
  height: '600px'
};

const center = {
  lat: 48.8566,   // Centered on Paris (Europe)
  lng: 2.3522
};

export default function GoogleMapComponent({ stations }: { stations: any[] }) {
  const context = useContext(WeatherContext);
  const [selected, setSelected] = useState<any>(null);
  const [filteredStations, setFilteredStations] = useState<any[]>([]);

  // Handle null context
  if (!context) return <div>Context not available</div>;

  const { filters } = context;

  // Filter stations based on context filters
  useEffect(() => {
    const filtered = stations.filter(station => {
      // Country filter
      if (filters.country && station.country !== filters.country) return false;

      // Date filter
      if (filters.startDate && station.time < filters.startDate) return false;
      if (filters.endDate && station.time > filters.endDate) return false;

      // PM2.5 range filter
      const pm25 = Number(station.pm25);
      if (pm25 < filters.pm25Range[0] || pm25 > filters.pm25Range[1]) return false;

      // PM10 range filter
      const pm10 = Number(station.pm10);
      if (pm10 < filters.pm10Range[0] || pm10 > filters.pm10Range[1]) return false;

      // Nitrogen Dioxide range filter
      const no2 = Number(station.nitrogendioxide);
      if (no2 < filters.nitrogendioxideRange[0] || no2 > filters.nitrogendioxideRange[1]) return false;

      // Ozone range filter
      const ozone = Number(station.ozone);
      if (ozone < filters.ozoneRange[0] || ozone > filters.ozoneRange[1]) return false;

      return true;
    });

    setFilteredStations(filtered);
  }, [stations, filters]);

  // Helper function to determine marker color based on PM2.5 level
  const getMarkerColor = (pm25: number) => {
    if (pm25 < 12) return 'http://maps.google.com/mapfiles/ms/icons/green-dot.png';
    if (pm25 < 35.4) return 'http://maps.google.com/mapfiles/ms/icons/yellow-dot.png';
    if (pm25 < 55.4) return 'http://maps.google.com/mapfiles/ms/icons/orange-dot.png';
    return 'http://maps.google.com/mapfiles/ms/icons/red-dot.png';
  };

  return (
    <div className="w-full">
      <div className="mb-4 text-sm text-gray-600">
        Showing {filteredStations.length} stations
      </div>

      <LoadScript googleMapsApiKey={process.env.NEXT_PUBLIC_GOOGLE_MAPS_API_KEY || ''}>
        <GoogleMap 
          mapContainerStyle={containerStyle} 
          center={center} 
          zoom={5}
          options={{
            styles: [
              {
                featureType: 'water',
                elementType: 'geometry',
                stylers: [{ color: '#e9e9e9' }, { lightness: 17 }]
              }
            ]
          }}
        >
          {/* Render markers for each filtered station */}
          {filteredStations.map((station, index) => (
            <Marker
              key={index}
              position={{
                lat: parseFloat(station.latitude),
                lng: parseFloat(station.longitude)
              }}
              icon={getMarkerColor(Number(station.pm25))}
              onClick={() => setSelected(station)}
              title={`${station.country} - PM2.5: ${station.pm25}`}
            />
          ))}

          {/* Info window shown on marker click */}
          {selected && (
            <InfoWindow
              position={{
                lat: parseFloat(selected.latitude),
                lng: parseFloat(selected.longitude)
              }}
              onCloseClick={() => setSelected(null)}
            >
              <div className="p-3 bg-white rounded shadow">
                <h3 className="font-bold text-lg mb-2">{selected.country}</h3>
                <div className="text-sm space-y-1">
                  <p><strong>Time:</strong> {selected.time}</p>
                  <p><strong>PM2.5:</strong> {selected.pm25} µg/m³</p>
                  <p><strong>PM10:</strong> {selected.pm10} µg/m³</p>
                  <p><strong>NO₂:</strong> {selected.nitrogendioxide} ppb</p>
                  <p><strong>Ozone:</strong> {selected.ozone} ppb</p>
                  <p><strong>Lat:</strong> {selected.latitude}</p>
                  <p><strong>Lng:</strong> {selected.longitude}</p>
                </div>
              </div>
            </InfoWindow>
          )}
        </GoogleMap>
      </LoadScript>
    </div>
  );
}
