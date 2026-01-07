import logging
from typing import Iterable, List

from .cities import City, DEFAULT_CITIES
from .client import AirQualityAPIError, fetch_city_measurements
from .open_meteo import OpenMeteoError, fetch_air_quality_forecast
from .pipeline import transform_measurements, transform_open_meteo_forecast
from .storage import AirQualityStorage

LOGGER = logging.getLogger(__name__)


class IngestionResult:
    def __init__(self) -> None:
        self.successes: List[str] = []
        self.failures: List[str] = []

    def report_success(self, city: City) -> None:
        self.successes.append(city.name)

    def report_failure(self, city: City, error: Exception) -> None:
        LOGGER.error("Failed to ingest data for %s: %s", city.name, error)
        self.failures.append(f"{city.name}: {error}")


def ingest_air_quality_data(
    *,
    api_key: str,
    storage: AirQualityStorage,
    cities: Iterable[City] | None = None,
    forecast_hours: int = 72,
    include_forecast: bool = True,
) -> IngestionResult:
    cities = list(cities) if cities else list(DEFAULT_CITIES)
    result = IngestionResult()
    for city in cities:
        observation_records = []
        forecast_records = []
        observation_error: Exception | None = None
        forecast_error: Exception | None = None
        try:
            payload = fetch_city_measurements(api_key, city)
            records = list(transform_measurements(city, payload))
            if records:
                observation_records = records
            else:
                observation_error = AirQualityAPIError(
                    "No observations returned for the specified window."
                )
        except AirQualityAPIError as api_error:
            observation_error = api_error
        except Exception as error:  # pylint: disable=broad-except
            observation_error = error

        if include_forecast:
            try:
                forecast_payload = fetch_air_quality_forecast(city, hours=forecast_hours)
                forecast_records = list(transform_open_meteo_forecast(city, forecast_payload))
                if not forecast_records:
                    forecast_error = OpenMeteoError(
                        "No forecast values returned for the requested horizon."
                    )
            except OpenMeteoError as api_error:
                forecast_error = api_error
            except Exception as error:  # pylint: disable=broad-except
                forecast_error = error

        total_records = 0
        if observation_records:
            storage.upsert_measurements(observation_records)
            total_records += len(observation_records)
            LOGGER.info("Ingested %d OpenAQ records for %s", len(observation_records), city.name)
        if forecast_records:
            storage.upsert_measurements(forecast_records)
            total_records += len(forecast_records)
            LOGGER.info(
                "Ingested %d Open-Meteo forecast records for %s",
                len(forecast_records),
                city.name,
            )

        if total_records:
            result.report_success(city)
            if observation_error:
                LOGGER.warning("OpenAQ ingest issues for %s: %s", city.name, observation_error)
            if forecast_error:
                LOGGER.warning("Open-Meteo ingest issues for %s: %s", city.name, forecast_error)
        else:
            error = observation_error or forecast_error or AirQualityAPIError(
                "No air-quality data ingested."
            )
            result.report_failure(city, error)
    return result
