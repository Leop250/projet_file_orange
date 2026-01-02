from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

import requests

from .cities import City

LOGGER = logging.getLogger(__name__)

API_BASE_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
DEFAULT_HOURLY_VARIABLES: List[str] = [
    "pm10",
    "pm2_5",
    "ozone",
    "nitrogen_dioxide",
    "sulphur_dioxide",
    "carbon_monoxide",
    "european_aqi",
]


class OpenMeteoError(Exception):
    """Raised when the Open-Meteo API call fails."""


def _normalize_hours_to_days(hours: int) -> int:
    """Open-Meteo expects an integer number of forecast days."""
    hours = max(1, min(hours, 168))  # Cap to one week for practicality.
    return max(1, math.ceil(hours / 24))


def _build_query(
    city: City,
    *,
    hours: int,
    hourly_variables: Optional[Iterable[str]] = None,
    timezone_name: str = "UTC",
) -> Dict[str, str]:
    variables = hourly_variables or DEFAULT_HOURLY_VARIABLES
    return {
        "latitude": f"{city.latitude:.4f}",
        "longitude": f"{city.longitude:.4f}",
        "hourly": ",".join(variables),
        "forecast_days": str(_normalize_hours_to_days(hours)),
        "timezone": timezone_name,
    }


def fetch_air_quality_forecast(
    city: City,
    *,
    hours: int = 72,
    hourly_variables: Optional[Iterable[str]] = None,
) -> Dict:
    """Fetch hourly air-quality forecast data from Open-Meteo."""
    query = _build_query(city, hours=hours, hourly_variables=hourly_variables)
    LOGGER.debug("Querying Open-Meteo for %s with %s", city.name, query)
    response = requests.get(API_BASE_URL, params=query, timeout=20)
    if response.status_code >= 400:
        raise OpenMeteoError(
            f"Open-Meteo API call failed with status {response.status_code}: {response.text}"
        )
    payload = response.json()
    if payload.get("error"):
        raise OpenMeteoError(
            f"Open-Meteo response returned error: {payload.get('reason', 'Unknown error')}"
        )
    return payload


def _timestamp_to_iso(timestamp: str) -> str:
    """Ensure timestamps returned by Open-Meteo are explicit UTC ISO strings."""
    if not timestamp:
        raise ValueError("Missing timestamp value in Open-Meteo payload.")
    # Open-Meteo returns timestamps without timezone when timezone=UTC.
    dt = datetime.fromisoformat(timestamp)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def extract_hourly_series(payload: Dict) -> Dict[str, List]:
    hourly = payload.get("hourly") or {}
    time_values = hourly.get("time") or []
    if not time_values:
        LOGGER.warning("No hourly time series available in Open-Meteo payload.")
        return {}

    series: Dict[str, List] = {"time": [_timestamp_to_iso(ts) for ts in time_values]}
    for key, values in hourly.items():
        if key == "time":
            continue
        if not isinstance(values, list):
            continue
        series[key] = values
    return series
