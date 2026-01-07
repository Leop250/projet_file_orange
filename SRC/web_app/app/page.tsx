'use client';

import { useEffect, useState, useMemo, useCallback } from 'react';
import dynamic from 'next/dynamic';
import GoogleMapComponent from '../components/google_map_component';
import { AirQualityData } from '@/types';

// Import graphique sans SSR (recharts nécessite window)
const AirQualityChart = dynamic(
  () => import('../components/air_quality_chart_component'),
  { ssr: false }
);

// =============================================================================
// UTILITAIRES
// =============================================================================

const formatDate = (dateString: string): string => {
  if (!dateString) return 'Date inconnue';
  const date = new Date(`${dateString}-01`);
  return new Intl.DateTimeFormat('fr-FR', {
    month: 'long',
    year: 'numeric',
  }).format(date);
};

const sortByDateDesc = (data: AirQualityData[]): AirQualityData[] => {
  return [...data].sort((a, b) => {
    if (a.is_realtime && !b.is_realtime) return -1;
    if (!a.is_realtime && b.is_realtime) return 1;
    return b.month.localeCompare(a.month);
  });
};

function groupDataByCity(data: AirQualityData[]): Record<string, AirQualityData[]> {
  const groups: Record<string, AirQualityData[]> = {};
  data.forEach(item => {
    const cityKey = item.city;
    if (!groups[cityKey]) groups[cityKey] = [];
    groups[cityKey].push(item);
  });
  Object.keys(groups).forEach(city => {
    groups[city] = sortByDateDesc(groups[city]);
  });
  return groups;
}

function getLatestStations(groupedData: Record<string, AirQualityData[]>): AirQualityData[] {
  return Object.values(groupedData)
    .map(group => {
      const realtimeData = group.find(item => item.is_realtime);
      return realtimeData || group[0];
    })
    .filter(Boolean);
}

const getPM25Color = (value: number): string => {
  if (value <= 10) return 'text-green-400';
  if (value <= 25) return 'text-yellow-400';
  if (value <= 50) return 'text-orange-400';
  return 'text-red-400';
};

const getTemperatureColor = (value: number): string => {
  if (value <= 0) return 'text-cyan-400';
  if (value <= 10) return 'text-blue-400';
  if (value <= 20) return 'text-green-400';
  if (value <= 30) return 'text-yellow-400';
  return 'text-red-400';
};

// Configuration des polluants pour l'affichage détaillé
const POLLUTANT_CONFIG = {
  pm2_5: { label: 'PM2.5', unit: 'µg/m³', color: 'text-red-400', bgColor: 'bg-red-500/20', icon: '🔴' },
  pm10: { label: 'PM10', unit: 'µg/m³', color: 'text-orange-400', bgColor: 'bg-orange-500/20', icon: '🟠' },
  no2: { label: 'NO₂', unit: 'µg/m³', color: 'text-yellow-400', bgColor: 'bg-yellow-500/20', icon: '🟡' },
  o3: { label: 'O₃', unit: 'µg/m³', color: 'text-green-400', bgColor: 'bg-green-500/20', icon: '🟢' },
  so2: { label: 'SO₂', unit: 'µg/m³', color: 'text-blue-400', bgColor: 'bg-blue-500/20', icon: '🔵' },
  co: { label: 'CO', unit: 'µg/m³', color: 'text-purple-400', bgColor: 'bg-purple-500/20', icon: '🟣' },
  nh3: { label: 'NH₃', unit: 'µg/m³', color: 'text-pink-400', bgColor: 'bg-pink-500/20', icon: '🩷' },
};

// Information AQI
const getAQIInfo = (aqi: number | null): { label: string; color: string; bgColor: string } => {
  if (aqi === null) return { label: 'N/A', color: 'text-gray-400', bgColor: 'bg-gray-500/20' };
  switch (aqi) {
    case 1: return { label: 'Bon', color: 'text-green-400', bgColor: 'bg-green-500/20' };
    case 2: return { label: 'Acceptable', color: 'text-yellow-400', bgColor: 'bg-yellow-500/20' };
    case 3: return { label: 'Modéré', color: 'text-orange-400', bgColor: 'bg-orange-500/20' };
    case 4: return { label: 'Mauvais', color: 'text-red-400', bgColor: 'bg-red-500/20' };
    case 5: return { label: 'Très mauvais', color: 'text-purple-400', bgColor: 'bg-purple-500/20' };
    default: return { label: 'N/A', color: 'text-gray-400', bgColor: 'bg-gray-500/20' };
  }
};

// =============================================================================
// COMPOSANTS
// =============================================================================

interface StatBoxProps {
  label: string;
  value: number;
  unit: string;
  color: string;
}

function StatBox({ label, value, unit, color }: StatBoxProps) {
  return (
    <div className="bg-white/5 p-2 rounded-lg border border-white/5">
      <div className="text-[8px] text-gray-400 uppercase font-bold tracking-wider mb-0.5">
        {label}
      </div>
      <div className="flex items-baseline gap-1">
        <span className={`font-mono text-lg font-bold ${color}`}>{value}</span>
        <span className="text-[8px] text-gray-500">{unit}</span>
      </div>
    </div>
  );
}

// Composant pour afficher un polluant individuel
interface PollutantCardProps {
  name: keyof typeof POLLUTANT_CONFIG;
  value: number;
}

function PollutantCard({ name, value }: PollutantCardProps) {
  const config = POLLUTANT_CONFIG[name];
  return (
    <div className={`${config.bgColor} p-2 rounded-lg border border-white/5 flex items-center justify-between`}>
      <div className="flex items-center gap-2">
        <span className="text-sm">{config.icon}</span>
        <span className="text-[10px] font-bold text-gray-300">{config.label}</span>
      </div>
      <div className="flex items-baseline gap-1">
        <span className={`font-mono text-sm font-bold ${config.color}`}>
          {value.toFixed(1)}
        </span>
        <span className="text-[8px] text-gray-500">{config.unit}</span>
      </div>
    </div>
  );
}

// Composant AQI Badge
interface AQIBadgeProps {
  aqi: number | null;
}

function AQIBadge({ aqi }: AQIBadgeProps) {
  const info = getAQIInfo(aqi);
  return (
    <div className={`${info.bgColor} p-3 rounded-xl border border-white/10 flex items-center justify-between`}>
      <div>
        <div className="text-[9px] text-gray-400 uppercase font-bold tracking-wider">
          Indice Qualité Air (AQI)
        </div>
        <div className={`text-lg font-bold ${info.color}`}>
          {info.label}
        </div>
      </div>
      <div className={`text-4xl font-bold ${info.color}`}>
        {aqi ?? '—'}
      </div>
    </div>
  );
}

// =============================================================================
// PAGE PRINCIPALE
// =============================================================================

// Type pour le mode d'affichage
type DisplayMode = 'pollution' | 'temperature';

export default function DashboardPage() {
  const [allData, setAllData] = useState<Record<string, AirQualityData[]>>({});
  const [latestStations, setLatestStations] = useState<AirQualityData[]>([]);
  const [selectedStation, setSelectedStation] = useState<AirQualityData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [displayMode, setDisplayMode] = useState<DisplayMode>('pollution');

  useEffect(() => {
    const fetchData = async () => {
      try {
        setError(null);
        const res = await fetch('/api/weather_data');

        if (!res.ok) {
          throw new Error(`Erreur serveur: ${res.status}`);
        }

        const data = await res.json();

        if (!Array.isArray(data)) {
          throw new Error('Format de données invalide');
        }

        const grouped = groupDataByCity(data);
        setAllData(grouped);

        const latest = getLatestStations(grouped);
        setLatestStations(latest);

        if (latest.length > 0) {
          setSelectedStation(latest[0]);
        }
      } catch (err) {
        console.error('Erreur API:', err);
        setError(err instanceof Error ? err.message : 'Erreur inconnue');
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, []);

  const selectedHistory = useMemo(() => {
    if (!selectedStation) return [];
    const history = allData[selectedStation.city] || [];
    return history;
  }, [selectedStation, allData]);

  const handleStationSelect = useCallback((station: AirQualityData) => {
    setSelectedStation(station);
  }, []);

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center h-screen w-screen bg-black text-white">
        <div className="w-16 h-16 border-4 border-blue-600 border-t-transparent rounded-full animate-spin" />
        <div className="mt-4 font-mono text-blue-400 text-sm animate-pulse tracking-widest">
          INITIALISATION...
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center h-screen w-screen bg-black text-white">
        <div className="text-red-500 text-xl mb-4">⚠️ Erreur</div>
        <div className="text-gray-400 text-sm">{error}</div>
        <button
          onClick={() => window.location.reload()}
          className="mt-4 px-4 py-2 bg-blue-600 rounded-lg hover:bg-blue-700 transition-colors"
        >
          Réessayer
        </button>
      </div>
    );
  }

  return (
    <main className="relative w-screen h-screen bg-black overflow-hidden font-sans text-white">
      {/* 1. LAYER CARTE */}
      <div className="absolute inset-0 z-0">
        <GoogleMapComponent
          stations={latestStations}
          onStationSelect={handleStationSelect}
          selectedStation={selectedStation}
          displayMode={displayMode}
        />
      </div>

      {/* 2. SIDEBAR GAUCHE */}
      <div className="absolute top-4 left-4 bottom-4 w-80 z-10 flex flex-col gap-4 pointer-events-none">
        <div className="glass-panel p-6 rounded-2xl pointer-events-auto border-l-4 border-blue-500 shadow-2xl">
          <h1 className="text-xl font-bold tracking-tight text-white">
            AeroScope - Observateur de l'air
          </h1>
          <p className="text-[10px] text-gray-400 uppercase tracking-wider font-mono mt-2">
            Réseau Hybride • {latestStations.length} Villes
          </p>
          
          {/* Toggle Pollution / Température */}
          <div className="flex mt-4 bg-white/5 rounded-lg p-1">
            <button
              onClick={() => setDisplayMode('pollution')}
              className={`flex-1 py-2 px-3 rounded-md text-xs font-bold uppercase tracking-wider transition-all ${
                displayMode === 'pollution'
                  ? 'bg-red-500/20 text-red-400 shadow-lg'
                  : 'text-gray-400 hover:text-white hover:bg-white/5'
              }`}
            >
              🌫️ Pollution
            </button>
            <button
              onClick={() => setDisplayMode('temperature')}
              className={`flex-1 py-2 px-3 rounded-md text-xs font-bold uppercase tracking-wider transition-all ${
                displayMode === 'temperature'
                  ? 'bg-blue-500/20 text-blue-400 shadow-lg'
                  : 'text-gray-400 hover:text-white hover:bg-white/5'
              }`}
            >
              🌡️ Température
            </button>
          </div>
        </div>

        <div className="glass-panel flex-1 rounded-2xl overflow-hidden flex flex-col pointer-events-auto shadow-2xl">
          <div className="p-3 border-b border-white/10 bg-white/5 flex justify-between px-4">
            <span className="text-[9px] font-bold uppercase tracking-widest text-gray-400">
              Ville
            </span>
            <span className="text-[9px] font-bold uppercase tracking-widest text-gray-400">
              {displayMode === 'pollution' ? 'Air (PM2.5)' : 'Temp (°C)'}
            </span>
          </div>

          <div className="overflow-y-auto flex-1 no-scrollbar p-2 space-y-1">
            {latestStations.map((station, idx) => (
              <div
                key={`${station.city}-${idx}`}
                onClick={() => handleStationSelect(station)}
                className={`
                  p-3 rounded-xl cursor-pointer transition-all border flex justify-between items-center group
                  ${selectedStation?.city === station.city
                    ? 'bg-blue-600/20 border-blue-500/50'
                    : 'bg-transparent border-transparent hover:bg-white/5'}
                `}
              >
                <div>
                  <div className="font-bold text-sm text-gray-200">{station.city}</div>
                  <div className="flex items-center gap-1">
                    <div className="text-[9px] text-gray-500 uppercase">{station.country}</div>
                    {station.is_realtime && (
                      <span className="text-[8px] bg-red-500/20 text-red-300 px-1 rounded ml-1 animate-pulse">
                        EN DIRECT
                      </span>
                    )}
                  </div>
                </div>
                <div className={`font-mono text-lg font-bold ${
                  displayMode === 'pollution' 
                    ? getPM25Color(station.pm2_5)
                    : getTemperatureColor(station.temperature_2m)
                }`}>
                  {displayMode === 'pollution' 
                    ? Math.round(station.pm2_5)
                    : `${Math.round(station.temperature_2m)}°`
                  }
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* 3. PANNEAU DROIT - Détails */}
      {selectedStation && (
        <div className="absolute top-4 right-4 bottom-4 z-10 w-[420px] animate-fade-in pointer-events-auto flex flex-col gap-3 overflow-hidden">
          
          {/* Bloc A : Header avec météo */}
          <div className="glass-panel rounded-2xl p-5 border-t-2 border-blue-500 shadow-2xl backdrop-blur-xl shrink-0">
            <div className="flex justify-between items-start mb-4">
              <div>
                <h2 className="text-2xl font-bold text-white">{selectedStation.city}</h2>
                <div className="flex items-center gap-2 mt-1">
                  <span className="text-[10px] uppercase tracking-wide text-gray-300 bg-white/10 px-2 py-0.5 rounded">
                    {selectedStation.weather_description}
                  </span>
                  {selectedStation.is_realtime && (
                    <span className="text-[8px] bg-red-500/30 text-red-300 px-2 py-0.5 rounded animate-pulse">
                      🔴 EN DIRECT
                    </span>
                  )}
                </div>
              </div>
              <div className="flex flex-col items-end">
                <div className="text-4xl font-bold tracking-tighter text-white">
                  {Math.round(selectedStation.temperature_2m)}°
                </div>
                <div className="text-[9px] text-gray-400 uppercase">Température</div>
              </div>
            </div>

            {/* Météo compacte */}
            <div className="grid grid-cols-3 gap-2">
              <StatBox
                label="Vent"
                value={selectedStation.wind_kph || 0}
                unit="km/h"
                color="text-cyan-300"
              />
              <StatBox
                label="Humidité"
                value={selectedStation.humidity || 0}
                unit="%"
                color="text-blue-300"
              />
              <StatBox
                label="Nuages"
                value={Math.round(selectedStation.cloudcover)}
                unit="%"
                color="text-gray-300"
              />
            </div>
          </div>

          {/* Bloc B : Qualité de l'air détaillée */}
          <div className="glass-panel rounded-2xl p-4 border-t-2 border-red-500/50 shadow-2xl backdrop-blur-xl shrink-0">
            <h3 className="text-xs font-bold uppercase tracking-widest text-red-300 mb-3">
              🌫️ Qualité de l'Air
            </h3>
            
            {/* AQI Badge */}
            <AQIBadge aqi={selectedStation.aqi} />
            
            {/* Grille des polluants */}
            <div className="grid grid-cols-2 gap-2 mt-3">
              <PollutantCard name="pm2_5" value={selectedStation.pm2_5} />
              <PollutantCard name="pm10" value={selectedStation.pm10} />
              <PollutantCard name="no2" value={selectedStation.no2} />
              <PollutantCard name="o3" value={selectedStation.o3} />
              <PollutantCard name="so2" value={selectedStation.so2} />
              <PollutantCard name="co" value={selectedStation.co} />
              <div className="col-span-2">
                <PollutantCard name="nh3" value={selectedStation.nh3} />
              </div>
            </div>
          </div>

          {/* Bloc C : Graphique & Historique */}
          <div className="glass-panel rounded-2xl flex-1 overflow-hidden flex flex-col border-t-2 border-purple-500/50 min-h-0">
            <div className="p-3 border-b border-white/10 bg-white/5 flex justify-between items-center shrink-0">
              <h3 className="text-xs font-bold uppercase tracking-widest text-purple-300">
                📊 Historique
              </h3>
              <span className="text-[9px] text-gray-500">
                {selectedHistory.length} entrées
              </span>
            </div>

            <div className="px-2 pt-2 shrink-0">
              <AirQualityChart data={selectedHistory} />
            </div>

            <div className="overflow-y-auto no-scrollbar p-2 space-y-2 flex-1 min-h-0">
              {selectedHistory.length === 0 ? (
                <div className="flex items-center justify-center h-full text-gray-500 text-sm">
                  Aucun historique disponible
                </div>
              ) : (
                selectedHistory.map((item, idx) => (
                  <div
                    key={`${item.month}-${idx}`}
                    className={`
                      flex justify-between items-center p-2 rounded-lg transition-colors border
                      ${item.is_realtime
                        ? 'bg-blue-500/10 border-blue-500/30 hover:bg-blue-500/20'
                        : 'bg-white/5 border-transparent hover:bg-white/10 hover:border-white/10'}
                    `}
                  >
                    <div className="flex flex-col">
                      <span className="font-mono text-xs font-bold text-gray-300 capitalize">
                        {formatDate(item.month)}
                      </span>
                      <span className={`text-[8px] uppercase truncate w-24 ${item.is_realtime ? 'text-blue-400' : 'text-gray-500'}`}>
                        {item.is_realtime ? '🔴 Temps Réel' : 'Archive Mensuelle'}
                      </span>
                    </div>

                    <div className="flex gap-3 text-right">
                      <div className="w-12">
                        <div className={`font-bold text-sm ${getPM25Color(item.pm2_5)}`}>
                          {Math.round(item.pm2_5)}
                        </div>
                        <div className="text-[8px] text-gray-600">PM2.5</div>
                      </div>
                      <div className="w-12">
                        <div className="font-bold text-sm text-blue-300">
                          {Math.round(item.temperature_2m)}°
                        </div>
                        <div className="text-[8px] text-gray-600">TEMP</div>
                      </div>
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>
      )}
    </main>
  );
}