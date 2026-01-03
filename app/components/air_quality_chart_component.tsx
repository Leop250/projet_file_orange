'use client';

import React from 'react';
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer
} from 'recharts';
import { AirQualityData } from '@/types';

interface ChartProps {
  data: AirQualityData[];
}

// Helper format court pour le graph (ex: "Juin")
const formatMonthShort = (dateString: string) => {
  if (!dateString) return "";
  const date = new Date(`${dateString}-01`);
  // Affiche "juin" ou "janv."
  return new Intl.DateTimeFormat('fr-FR', { month: 'short', year: '2-digit' }).format(date);
};

export default function AirQualityChart({ data }: ChartProps) {
  if (!data || data.length === 0) {
    return (
      <div className="flex items-center justify-center h-full w-full text-xs text-gray-500 font-mono">
        Pas d'historique
      </div>
    );
  }

  const chartData = [...data].reverse().map(item => ({
    month: item.month,
    pm25: Math.round(item.pm2_5 || 0),
    temp: Math.round(item.temperature_2m || 0)
  }));

  return (
    <div className="w-full h-48 mt-4 select-none">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart
          data={chartData}
          margin={{ top: 10, right: 10, left: -20, bottom: 0 }}
        >
          <defs>
            <linearGradient id="colorPm25" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#ef4444" stopOpacity={0.4}/>
              <stop offset="95%" stopColor="#ef4444" stopOpacity={0}/>
            </linearGradient>
            <linearGradient id="colorTemp" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.4}/>
              <stop offset="95%" stopColor="#3b82f6" stopOpacity={0}/>
            </linearGradient>
          </defs>

          <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" vertical={false} />

          <XAxis 
            dataKey="month" 
            tick={{ fill: '#6b7280', fontSize: 9, fontFamily: 'monospace' }} 
            // Utilisation du formateur court
            tickFormatter={formatMonthShort} 
            interval="preserveStartEnd"
            axisLine={false}
            tickLine={false}
            dy={5}
          />

          <YAxis 
            tick={{ fill: '#6b7280', fontSize: 9, fontFamily: 'monospace' }} 
            axisLine={false}
            tickLine={false}
            dx={-5}
          />

          <Tooltip 
            contentStyle={{ 
              backgroundColor: 'rgba(20, 20, 20, 0.95)', 
              borderColor: 'rgba(255,255,255,0.1)', 
              borderRadius: '8px',
              fontSize: '11px'
            }}
            // Formatage de la date dans le tooltip (ex: Juin 2024)
            labelFormatter={(label) => new Date(`${label}-01`).toLocaleDateString('fr-FR', { month: 'long', year: 'numeric' })}
            formatter={(value: number | undefined, name: string | undefined) => [
              value, 
              name === 'pm25' ? 'PM2.5 (µg/m³)' : 'Température (°C)'
            ]}
          />

          <Area 
            type="monotone" 
            dataKey="pm25" 
            stroke="#ef4444" 
            strokeWidth={2}
            fillOpacity={1} 
            fill="url(#colorPm25)" 
            name="pm25"
          />

          <Area 
            type="monotone" 
            dataKey="temp" 
            stroke="#3b82f6" 
            strokeWidth={2}
            fillOpacity={1} 
            fill="url(#colorTemp)" 
            name="temp"
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}