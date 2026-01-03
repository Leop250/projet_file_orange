'use client';

import React, { useState, useCallback } from 'react';
import { GoogleMap, useJsApiLoader, OverlayView } from '@react-google-maps/api';
import { AirQualityData } from '@/types';

const mapStyles = [
  { elementType: "geometry", stylers: [{ color: "#242f3e" }] },
  { elementType: "labels.text.stroke", stylers: [{ color: "#242f3e" }] },
  { elementType: "labels.text.fill", stylers: [{ color: "#746855" }] },
  { featureType: "water", elementType: "geometry", stylers: [{ color: "#17263c" }] },
];

const containerStyle = { width: '100%', height: '100vh' };
const defaultCenter = { lat: 48.8566, lng: 2.3522 };

interface MapProps {
  stations: AirQualityData[];
  onStationSelect: (station: AirQualityData) => void;
  selectedStation: AirQualityData | null;
}

export default function GoogleMapComponent({ stations, onStationSelect, selectedStation }: MapProps) {
  const { isLoaded, loadError } = useJsApiLoader({
    id: 'google-map-script',
    googleMapsApiKey: process.env.NEXT_PUBLIC_GOOGLE_MAPS_API_KEY || ""
  });

  const [map, setMap] = useState<google.maps.Map | null>(null);

  const onLoad = useCallback((map: google.maps.Map) => setMap(map), []);
  const onUnmount = useCallback(() => setMap(null), []);

  const getPollutionColor = (pm25: number) => {
    if (pm25 <= 10) return "bg-teal-500 border-teal-300 shadow-[0_0_15px_rgba(20,184,166,0.6)]";
    if (pm25 <= 25) return "bg-yellow-500 border-yellow-300 shadow-[0_0_15px_rgba(234,179,8,0.6)]";
    if (pm25 <= 50) return "bg-orange-500 border-orange-300 shadow-[0_0_15px_rgba(249,115,22,0.6)]";
    return "bg-red-600 border-red-400 shadow-[0_0_20px_rgba(220,38,38,0.8)] animate-pulse";
  };

  if (loadError) return <div className="flex items-center justify-center h-full bg-black text-red-500">Erreur Clé API</div>;
  if (!isLoaded) return <div className="flex items-center justify-center h-full bg-black text-blue-500 animate-pulse">Chargement Carte...</div>;

  return (
    <GoogleMap
      mapContainerStyle={containerStyle}
      center={defaultCenter}
      zoom={5}
      onLoad={onLoad}
      onUnmount={onUnmount}
      options={{
        styles: mapStyles,
        disableDefaultUI: true,
        zoomControl: false,
        minZoom: 3,
      }}
    >
      {Array.isArray(stations) && stations.map((station, index) => (
        <OverlayView
          key={`${station.city}-${index}`}
          position={{ lat: station.latitude, lng: station.longitude }}
          mapPaneName={OverlayView.OVERLAY_MOUSE_TARGET}
        >
          <div
            onClick={() => onStationSelect(station)}
            title={`PM2.5: ${Math.round(station.pm2_5)} | ${station.is_realtime ? 'LIVE WEATHER' : 'ARCHIVE'}`}
            className={`
              cursor-pointer flex items-center justify-center transition-all duration-300
              w-10 h-10 rounded-full border-2 backdrop-blur-md relative group
              ${getPollutionColor(station.pm2_5)}
              ${selectedStation?.city === station.city ? 'scale-125 z-50 ring-4 ring-white/50' : 'hover:scale-110 opacity-90'}
            `}
          >
            {/* Indicateur LIVE (petit point rouge si temps réel) */}
            {station.is_realtime && (
               <span className="absolute -top-1 -right-1 w-3 h-3 bg-red-500 rounded-full border border-white animate-pulse"></span>
            )}

            <span className="font-bold text-[10px] text-white drop-shadow-md">
              {Math.round(station.pm2_5)}
            </span>
          </div>
        </OverlayView>
      ))}
    </GoogleMap>
  );
}