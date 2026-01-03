export interface AirQualityData {
  // Identification
  city: string;
  country: string;
  latitude: number;
  longitude: number;
  month: string; // "YYYY-MM"
  
  pm2_5: number;
  pm10: number;
  ozone: number;
  nitrogen_dioxide: number;
  
  temperature_2m: number;
  cloudcover: number;
  weather_description: string;

  wind_kph?: number;
  humidity?: number;
  is_realtime?: boolean;
}