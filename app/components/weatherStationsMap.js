import { useEffect, useState } from 'react';
import { GoogleMap, LoadScript, Marker, InfoWindow } from '@react-google-maps/api';

const containerStyle = {
  width: '100%',
  height: '600px'
};

const center = {
  lat: 48.8566,   
  lng: 2.3522
};

function WeatherStationsMap({ stations }) {
  const [selected, setSelected] = useState(null);

  return (
    <LoadScript googleMapsApiKey={process.env.NEXT_PUBLIC_GOOGLE_MAPS_API_KEY}>
      <GoogleMap mapContainerStyle={containerStyle} center={center} zoom={5}>
        {stations.map((station, index) => (
          <Marker 
            key={index} 
            position={{ lat: parseFloat(station.latitude), lng: parseFloat(station.longitude) }}
            onClick={() => setSelected(station)}
          />
        ))}

        {selected && (
          <InfoWindow position={{ lat: parseFloat(selected.latitude), lng: parseFloat(selected.longitude) }} onCloseClick={() => setSelected(null)}>
            <div>
              <h4>{selected.country}</h4>
              <p>PM2.5: {selected.pm25}</p>
              <p>PM10: {selected.pm10}</p>
              <p>Ozone: {selected.ozone}</p>
              <p>Time: {selected.time}</p>
            </div>
          </InfoWindow>
        )}
      </GoogleMap>
    </LoadScript>
  );
}

export default WeatherStationsMap;
