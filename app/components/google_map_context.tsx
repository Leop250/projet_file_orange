'use client';

import React, { createContext, useState, Dispatch, SetStateAction } from 'react';

type Filters = {
  country: string | null;
  startDate: string | null;    // or Date, but string works well for date pickers
  endDate: string | null;
  pm25Range: [number, number];
  pm10Range: [number, number];
  nitrogendioxideRange: [number, number];
  ozoneRange: [number, number];
};

type WeatherContextType = {
  filters: Filters;
  setFilters: Dispatch<SetStateAction<Filters>>;
  // Optionally store filteredResults or rawData if you want
};

const defaultFilters: Filters = {
  country: null,
  startDate: null,
  endDate: null,
  pm25Range: [0, 100],
  pm10Range: [0, 100],
  nitrogendioxideRange: [0, 100],
  ozoneRange: [0, 100]
};

export const WeatherContext = createContext<WeatherContextType | null>(null);

export function ContextProvider({ children }: { children: React.ReactNode }) {
  const [filters, setFilters] = useState<Filters>(defaultFilters);

  return (
    <WeatherContext.Provider value={{ filters, setFilters }}>
      {children}
    </WeatherContext.Provider>
  );
}
