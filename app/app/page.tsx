'use client';

import { useEffect, useState, useMemo, useCallback } from 'react';
import dynamic from 'next/dynamic';
import GoogleMapComponent from '../components/google_map_component';
import { AirQualityData, GroupedAirQualityData, PM25_THRESHOLDS } from '@/types';

// Import graphique sans SSR (recharts nécessite window)
const AirQualityChart = dynamic(
  () => import('../components/air_quality_chart_component'),
  { ssr: false }
);

// =============================================================================
// UTILITAIRES
// =============================================================================

/**
 * Formate une date "YYYY-MM" en "Juin 2024"
 */
const formatDate = (dateString: string): string => {
  if (!dateString) return 'Date inconnue';
  const date = new Date(`${dateString}-01`);
  return new Intl.DateTimeFormat('fr-FR', {
    month: 'long',
    year: 'numeric',
  }).format(date);
};

/**
 * Trie les données : temps réel en premier, puis par mois décroissant
 */
const sortByDateDesc = (data: AirQualityData[]): AirQualityData[] => {
  return [...data].sort((a, b) => {
    // Temps réel toujours en premier
    if (a.is_realtime && !b.is_realtime) return -1;
    if (!a.is_realtime && b.is_realtime) return 1;
    // Puis tri par mois décroissant
    return b.month.localeCompare(a.month);
  });
};

/**
 * Groupe les données par ville et trie chaque groupe
 */
const groupDataByCity = (data: AirQualityData[]): GroupedAirQualityData => {
  const groups: GroupedAirQualityData = {};

  data.forEach(item => {
    const cityKey = item.city;
    if (!groups[cityKey]) groups[cityKey] = [];
    groups[cityKey].push(item);
  });

  // Trier chaque groupe
  Object.keys(groups).forEach(city => {
    groups[city] = sortByDateDesc(groups[city]);
  });

  return groups;
};

/**
 * Extrait la donnée la plus récente (temps réel) pour chaque ville
 */
const getLatestStations = (groupedData: GroupedAirQualityData): AirQualityData[] => {
  return Object.values(groupedData)
    .map(group => {
      // Chercher d'abord une donnée temps réel
      const realtimeData = group.find(item => item.is_realtime);
      return realtimeData || group[0];
    })
    .filter(Boolean);
};

/**
 * Retourne la couleur selon le niveau de PM2.5
 */
const getPM25Color = (value: number): string => {
  if (value <= PM25_THRESHOLDS.GOOD) return 'text-green-400';
  if (value <= PM25_THRESHOLDS.MODERATE) return 'text-yellow-400';
  if (value <= PM25_THRESHOLDS.UNHEALTHY) return 'text-orange-400';
  return 'text-red-400';
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

interface HistoryItemProps {
  item: AirQualityData;
}

function HistoryItem({ item }: HistoryItemProps) {
  return (
    <div
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
  );
}

// =============================================================================
// PAGE PRINCIPALE
// =============================================================================

export default function DashboardPage() {
  const [allData, setAllData] = useState<GroupedAirQualityData>({});
  const [latestStations, setLatestStations] = useState<AirQualityData[]>([]);
  const [selectedStation, setSelectedStation] = useState<AirQualityData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Fetch des données
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

  // Historique trié pour la station sélectionnée
  const selectedHistory = useMemo(() => {
    if (!selectedStation) return [];
    return allData[selectedStation.city] || [];
  }, [selectedStation, allData]);

  // Handler de sélection mémorisé
  const handleStationSelect = useCallback((station: AirQualityData) => {
    setSelectedStation(station);
  }, []);

  // État de chargement
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

  // État d'erreur
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
        />
      </div>

      {/* 2. SIDEBAR GAUCHE */}
      <div className="absolute top-4 left-4 bottom-4 w-80 z-10 flex flex-col gap-4 pointer-events-none">
        {/* Header */}
        <div className="glass-panel p-6 rounded-2xl pointer-events-auto border-l-4 border-blue-500 shadow-2xl">
          <h1 className="text-xl font-bold tracking-tight text-white">
            AIR OBSERVATORY
          </h1>
          <p className="text-[10px] text-gray-400 uppercase tracking-wider font-mono mt-2">
            Réseau Hybride • {latestStations.length} Villes
          </p>
        </div>

        {/* Liste des villes */}
        <div className="glass-panel flex-1 rounded-2xl overflow-hidden flex flex-col pointer-events-auto shadow-2xl">
          <div className="p-3 border-b border-white/10 bg-white/5 flex justify-between px-4">
            <span className="text-[9px] font-bold uppercase tracking-widest text-gray-400">
              Ville
            </span>
            <span className="text-[9px] font-bold uppercase tracking-widest text-gray-400">
              Air (PM2.5)
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
                        LIVE
                      </span>
                    )}
                  </div>
                </div>
                <div className={`font-mono text-lg font-bold ${getPM25Color(station.pm2_5)}`}>
                  {Math.round(station.pm2_5)}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* 3. PANNEAU DROIT - Détails */}
      {selectedStation && (
        <div className="absolute top-4 right-4 bottom-4 z-10 w-96 animate-fade-in pointer-events-auto flex flex-col gap-4">
          {/* Bloc A : Données actuelles */}
          <div className="glass-panel rounded-2xl p-6 border-t-2 border-blue-500 shadow-2xl backdrop-blur-xl shrink-0">
            <div className="flex justify-between items-start mb-6">
              <div>
                <h2 className="text-3xl font-bold text-white">{selectedStation.city}</h2>
                <div className="flex items-center gap-2 mt-1">
                  <span className="text-[10px] uppercase tracking-wide text-gray-300 bg-white/10 px-2 py-0.5 rounded">
                    {selectedStation.weather_description}
                  </span>
                </div>
              </div>
              <div className="flex flex-col items-end">
                <div className="text-5xl font-bold tracking-tighter text-white">
                  {Math.round(selectedStation.temperature_2m)}°
                </div>
                <div className="text-[9px] text-gray-400 uppercase">Température</div>
              </div>
            </div>

            <div className="grid grid-cols-2 gap-3 mb-4">
              <StatBox
                label="PM2.5 (Pollution)"
                value={Math.round(selectedStation.pm2_5)}
                unit="µg/m³"
                color={getPM25Color(selectedStation.pm2_5)}
              />
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

          {/* Bloc B : Graphique & Historique */}
          <div className="glass-panel rounded-2xl flex-1 overflow-hidden flex flex-col border-t-2 border-purple-500/50">
            <div className="p-4 border-b border-white/10 bg-white/5 flex justify-between items-center">
              <h3 className="text-xs font-bold uppercase tracking-widest text-purple-300">
                Historique (24 Mois)
              </h3>
              <span className="text-[9px] text-gray-500">
                {selectedHistory.length} entrées
              </span>
            </div>

            <div className="px-2 pt-2">
              <AirQualityChart data={selectedHistory} />
            </div>

            <div className="overflow-y-auto no-scrollbar p-2 space-y-2 flex-1">
              {selectedHistory.length === 0 ? (
                <div className="flex items-center justify-center h-full text-gray-500 text-sm">
                  Aucun historique disponible
                </div>
              ) : (
                selectedHistory.map((item, idx) => (
                  <HistoryItem key={`${item.month}-${idx}`} item={item} />
                ))
              )}
            </div>
          </div>
        </div>
      )}
    </main>
  );
}