import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple

import requests

from .cities import City

LOGGER = logging.getLogger(__name__)

API_BASE_URL = "https://api.openaq.org/v3"
DEFAULT_PARAMETERS: List[str] = ["pm25", "pm10", "o3", "no2", "so2", "co"]
MAX_RADIUS_METERS = 25_000


class AirQualityAPIError(Exception):
    """Raised when the OpenAQ API call fails."""


@dataclass(frozen=True)
class SensorSelection:
    parameter: str
    sensor_id: int
    location_id: int
    location_name: str
    coordinates: Dict[str, float] | None
    distance: float | None
    units: str | None


def _get(endpoint: str, api_key: Optional[str], params: Iterable[tuple[str, str]]) -> Dict:
    url = f"{API_BASE_URL}/{endpoint}"
    headers = {
        "Accept": "application/json",
    }
    if api_key:
        headers["X-API-Key"] = api_key
    response = requests.get(url, params=list(params), headers=headers, timeout=20)
    if response.status_code >= 400:
        raise AirQualityAPIError(
            f"OpenAQ API call failed with status {response.status_code}: {response.text}"
        )
    return response.json()


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00") if value.endswith("Z") else value
        return datetime.fromisoformat(normalized)
    except ValueError:
        LOGGER.debug("Unable to parse datetime string from OpenAQ payload: %s", value)
        return None


def _location_sort_key(location: Dict) -> Tuple[float, float]:
    last_dt = _parse_iso_datetime(
        (location.get("datetimeLast") or {}).get("utc")
    )
    timestamp_key = -last_dt.timestamp() if last_dt else float("inf")
    distance = location.get("distance") or float("inf")
    return (timestamp_key, distance)


def _select_sensors_for_parameters(
    locations: List[Dict],
    parameters: List[str],
) -> Dict[str, SensorSelection]:
    selections: Dict[str, SensorSelection] = {}
    for location in sorted(locations, key=_location_sort_key):
        sensors = location.get("sensors") or []
        for sensor in sensors:
            parameter_name = (sensor.get("parameter") or {}).get("name")
            if not parameter_name:
                continue
            parameter_name = parameter_name.lower()
            if parameter_name not in parameters or parameter_name in selections:
                continue
            selections[parameter_name] = SensorSelection(
                parameter=parameter_name,
                sensor_id=int(sensor["id"]),
                location_id=int(location["id"]),
                location_name=location.get("name") or "",
                coordinates=location.get("coordinates"),
                distance=location.get("distance"),
                units=(sensor.get("parameter") or {}).get("units"),
            )
        if len(selections) == len(parameters):
            break
    return selections


def _fetch_sensor_measurements(
    api_key: Optional[str],
    selection: SensorSelection,
    *,
    start_time: datetime,
    end_time: datetime,
    limit: int,
) -> List[Dict]:
    response = _get(
        f"sensors/{selection.sensor_id}/measurements",
        api_key,
        [
            ("datetime_from", start_time.isoformat()),
            ("datetime_to", end_time.isoformat()),
            ("limit", str(limit)),
            ("page", "1"),
        ],
    )
    results = response.get("results") or []
    measurements: List[Dict] = []
    for entry in results:
        measurement_time = (
            entry.get("period", {}).get("datetimeTo", {}).get("utc")
            or entry.get("period", {}).get("datetimeFrom", {}).get("utc")
        )
        if not measurement_time:
            continue
        measurements.append(
            {
                "parameter": selection.parameter,
                "value": entry.get("value"),
                "datetime": measurement_time,
                "location": {
                    "id": selection.location_id,
                    "name": selection.location_name,
                    "coordinates": selection.coordinates,
                    "distance": selection.distance,
                },
                "sensor": {
                    "id": selection.sensor_id,
                    "units": entry.get("parameter", {}).get("units") or selection.units,
                },
                "raw": entry,
            }
        )
    return measurements


def fetch_city_measurements(
    api_key: Optional[str],
    city: City,
    *,
    hours: int = 24,
    parameters: Optional[Iterable[str]] = None,
    radius_km: int = 25,
    sensor_measurement_limit: int | None = None,
) -> Dict:
    """Fetch recent pollutant measurements around a city using OpenAQ v3 endpoints."""
    selected_parameters = [param.lower() for param in (parameters or DEFAULT_PARAMETERS)]
    end_time = datetime.now(tz=timezone.utc)
    start_time = end_time - timedelta(hours=max(hours, 1))
    radius_meters = min(max(radius_km * 1000, 1000), MAX_RADIUS_METERS)
    LOGGER.debug(
        "Looking up locations near %s within %dm for parameters %s",
        city.name,
        radius_meters,
        ",".join(selected_parameters),
    )
    locations_response = _get(
        "locations",
        api_key,
        [
            ("coordinates", f"{city.latitude:.4f},{city.longitude:.4f}"),
            ("radius", str(int(radius_meters))),
            ("limit", "100"),
            ("page", "1"),
        ],
    )
    locations = locations_response.get("results") or []
    if not locations:
        raise AirQualityAPIError(
            f"No monitoring locations found near {city.name} ({city.latitude}, {city.longitude})."
        )
    selections = _select_sensors_for_parameters(locations, selected_parameters)
    if not selections:
        raise AirQualityAPIError(
            "No suitable sensors found for requested parameters near the selected city."
        )
    limit = sensor_measurement_limit or max(24, min(hours * 3, 200))
    aggregated: List[Dict] = []
    for parameter, selection in selections.items():
        LOGGER.debug(
            "Fetching measurements for %s via sensor %s at location %s",
            parameter,
            selection.sensor_id,
            selection.location_name,
        )
        sensor_measurements = _fetch_sensor_measurements(
            api_key,
            selection,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )
        aggregated.extend(sensor_measurements)
    return {"results": aggregated}
