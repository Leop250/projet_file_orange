import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Make sure all required weather variables are listed here
url = "https://air-quality-api.open-meteo.com/v1/air-quality"
params = {
    "latitude": 52.52,
    "longitude": 13.41,
    "hourly": ["pm10", "pm2_5", "carbon_monoxide", "carbon_dioxide", "sulphur_dioxide", "nitrogen_dioxide",
               "ozone", "aerosol_optical_depth", "dust", "uv_index", "uv_index_clear_sky", "ammonia",
               "methane", "ragweed_pollen", "olive_pollen", "mugwort_pollen", "grass_pollen", "birch_pollen",
               "alder_pollen", "european_aqi", "european_aqi_pm2_5", "european_aqi_pm10",
               "european_aqi_nitrogen_dioxide", "european_aqi_ozone", "european_aqi_sulphur_dioxide",
               "formaldehyde", "glyoxal", "non_methane_volatile_organic_compounds", "pm10_wildfires",
               "peroxyacyl_nitrates", "secondary_inorganic_aerosol", "nitrogen_monoxide", "sea_salt_aerosol",
               "pm2_5_total_organic_matter", "total_elementary_carbon", "residential_elementary_carbon"],
    "current": ["european_aqi", "pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide", "sulphur_dioxide",
                "ozone", "aerosol_optical_depth", "dust", "uv_index", "uv_index_clear_sky", "ammonia",
                "alder_pollen", "birch_pollen", "mugwort_pollen", "grass_pollen", "olive_pollen", "ragweed_pollen"],
    "start_date": "2022-01-01",
    "end_date": "2025-11-10"
}
responses = openmeteo.weather_api(url, params=params)

# Liste pour stocker tous les DataFrames
all_dataframes = []

# Process each location response
for response in responses:
    print(f"\nCoordinates: {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation: {response.Elevation()} m asl")
    print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

    # Process current data
    current = response.Current()
    current_time = current.Time()
    current_european_aqi = current.Variables(0).Value()
    current_pm10 = current.Variables(1).Value()
    current_pm2_5 = current.Variables(2).Value()
    current_carbon_monoxide = current.Variables(3).Value()
    current_nitrogen_dioxide = current.Variables(4).Value()
    current_sulphur_dioxide = current.Variables(5).Value()
    current_ozone = current.Variables(6).Value()
    current_aerosol_optical_depth = current.Variables(7).Value()
    current_dust = current.Variables(8).Value()
    current_uv_index = current.Variables(9).Value()
    current_uv_index_clear_sky = current.Variables(10).Value()
    current_ammonia = current.Variables(11).Value()
    current_alder_pollen = current.Variables(12).Value()
    current_birch_pollen = current.Variables(13).Value()
    current_mugwort_pollen = current.Variables(14).Value()
    current_grass_pollen = current.Variables(15).Value()
    current_olive_pollen = current.Variables(16).Value()
    current_ragweed_pollen = current.Variables(17).Value()

    print(f"Current time: {current_time}")
    print(f"Current european_aqi: {current_european_aqi}")

    # Process hourly data
    hourly = response.Hourly()
    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        ),
        "latitude": response.Latitude(),
        "longitude": response.Longitude(),
        "elevation": response.Elevation(),
        "pm10": hourly.Variables(0).ValuesAsNumpy(),
        "pm2_5": hourly.Variables(1).ValuesAsNumpy(),
        "carbon_monoxide": hourly.Variables(2).ValuesAsNumpy(),
        "carbon_dioxide": hourly.Variables(3).ValuesAsNumpy(),
        "sulphur_dioxide": hourly.Variables(4).ValuesAsNumpy(),
        "nitrogen_dioxide": hourly.Variables(5).ValuesAsNumpy(),
        "ozone": hourly.Variables(6).ValuesAsNumpy(),
        "aerosol_optical_depth": hourly.Variables(7).ValuesAsNumpy(),
        "dust": hourly.Variables(8).ValuesAsNumpy(),
        "uv_index": hourly.Variables(9).ValuesAsNumpy(),
        "uv_index_clear_sky": hourly.Variables(10).ValuesAsNumpy(),
        "ammonia": hourly.Variables(11).ValuesAsNumpy(),
        "methane": hourly.Variables(12).ValuesAsNumpy(),
        "ragweed_pollen": hourly.Variables(13).ValuesAsNumpy(),
        "olive_pollen": hourly.Variables(14).ValuesAsNumpy(),
        "mugwort_pollen": hourly.Variables(15).ValuesAsNumpy(),
        "grass_pollen": hourly.Variables(16).ValuesAsNumpy(),
        "birch_pollen": hourly.Variables(17).ValuesAsNumpy(),
        "alder_pollen": hourly.Variables(18).ValuesAsNumpy(),
        "european_aqi": hourly.Variables(19).ValuesAsNumpy(),
        "european_aqi_pm2_5": hourly.Variables(20).ValuesAsNumpy(),
        "european_aqi_pm10": hourly.Variables(21).ValuesAsNumpy(),
        "european_aqi_nitrogen_dioxide": hourly.Variables(22).ValuesAsNumpy(),
        "european_aqi_ozone": hourly.Variables(23).ValuesAsNumpy(),
        "european_aqi_sulphur_dioxide": hourly.Variables(24).ValuesAsNumpy(),
        "formaldehyde": hourly.Variables(25).ValuesAsNumpy(),
        "glyoxal": hourly.Variables(26).ValuesAsNumpy(),
        "non_methane_volatile_organic_compounds": hourly.Variables(27).ValuesAsNumpy(),
        "pm10_wildfires": hourly.Variables(28).ValuesAsNumpy(),
        "peroxyacyl_nitrates": hourly.Variables(29).ValuesAsNumpy(),
        "secondary_inorganic_aerosol": hourly.Variables(30).ValuesAsNumpy(),
        "nitrogen_monoxide": hourly.Variables(31).ValuesAsNumpy(),
        "sea_salt_aerosol": hourly.Variables(32).ValuesAsNumpy(),
        "pm2_5_total_organic_matter": hourly.Variables(33).ValuesAsNumpy(),
        "total_elementary_carbon": hourly.Variables(34).ValuesAsNumpy(),
        "residential_elementary_carbon": hourly.Variables(35).ValuesAsNumpy()
    }

    hourly_dataframe = pd.DataFrame(data=hourly_data)
    all_dataframes.append(hourly_dataframe)
    print(f"Données horaires ajoutées pour cette localisation")

# Fusionner tous les DataFrames et exporter vers CSV
final_dataframe = pd.concat(all_dataframes, ignore_index=True)
final_dataframe.to_csv('open_meteo.csv', index=False, encoding='utf-8')

print(f"\n✅ Export CSV terminé : open_meteo.csv ({len(final_dataframe)} lignes)")
