export interface WeatherStation {
  country: string;
  city?: string;
  location_name?: string;
  time: string;
  latitude: string;
  longitude: string;

  pm25?: string;
  pm10?: string;
  nitrogendioxide?: string;
  ozone?: string;
  
  // Index signature pour permettre l'accès dynamique si nécessaire (optionnel mais utile avec CSV)
  [key: string]: string | undefined;
}