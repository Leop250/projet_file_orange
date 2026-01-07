from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, Iterable, List

from .cities import City
from .open_meteo import extract_hourly_series

LOGGER = logging.getLogger(__name__)

POLLUTANT_CODE_MAP = {
    "pm25": "pm25",
    "pm10": "pm10",
    "o3": "o3",
    "no2": "no2",
    "so2": "so2",
    "co": "co",
}


def _utcnow_iso() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()


def _dominant_pollutant(values: Dict[str, float]) -> str | None:
    populated = {key: value for key, value in values.items() if value is not None}
    if not populated:
        return None
    return max(populated, key=populated.get)


def transform_measurements(city: City, payload: Dict) -> Iterable[Dict]:
    """Transform OpenAQ measurement payload into storage-friendly records."""
    aggregated: Dict[str, Dict] = {}
    results = payload.get("results", [])
    for entry in results:
        parameter = (entry.get("parameter") or "").lower()
        target_key = POLLUTANT_CODE_MAP.get(parameter)
        if not target_key:
            continue
        measured_at = entry.get("datetime")
        if not measured_at:
            continue
        record = aggregated.setdefault(
            measured_at,
            {
                "city": city.name,
                "country": city.country,
                "measured_at": measured_at,
                "source": "openaq",
                "aqi": None,
                "category": None,
                "dominant_pollutant": None,
                "received_at": _utcnow_iso(),
                "health_recommendations": "",
                "raw_payload": [],
                **{value: None for value in POLLUTANT_CODE_MAP.values()},
            },
        )
        value = entry.get("value")
        record[target_key] = float(value) if value is not None else None
        record["raw_payload"].append(entry)

    for timestamp in sorted(aggregated.keys()):
        record = aggregated[timestamp]
        pollutant_values = {
            key: record.get(key)
            for key in POLLUTANT_CODE_MAP.values()
            if record.get(key) is not None
        }
        record["dominant_pollutant"] = _dominant_pollutant(pollutant_values)
        record["raw_payload"] = {"measurements": record["raw_payload"]}
        yield record


OPEN_METEO_KEY_MAP = {
    "pm10": "pm10",
    "pm2_5": "pm25",
    "ozone": "o3",
    "nitrogen_dioxide": "no2",
    "sulphur_dioxide": "so2",
    "carbon_monoxide": "co",
}


def transform_open_meteo_forecast(city: City, payload: Dict) -> Iterable[Dict]:
    """Convert Open-Meteo hourly forecast data into storage-friendly records."""
    series = extract_hourly_series(payload)
    if not series:
        return []
    timestamps = series.get("time") or []
    records: List[Dict] = []
    for idx, timestamp in enumerate(timestamps):
        record = {
            "city": city.name,
            "country": city.country,
            "measured_at": timestamp,
            "source": "open-meteo",
            "aqi": None,
            "category": None,
            "dominant_pollutant": None,
            "received_at": _utcnow_iso(),
            "health_recommendations": "",
            "raw_payload": {},
            **{value: None for value in POLLUTANT_CODE_MAP.values()},
        }
        pollutant_values = {}
        for api_key, target_key in OPEN_METEO_KEY_MAP.items():
            values = series.get(api_key)
            if values is None or idx >= len(values):
                continue
            value = values[idx]
            if value is None:
                continue
            try:
                numeric_value = float(value)
            except (TypeError, ValueError):
                continue
            record[target_key] = numeric_value
            pollutant_values[target_key] = numeric_value
        aqi_values = series.get("european_aqi")
        if aqi_values is not None and idx < len(aqi_values):
            aqi_value = aqi_values[idx]
            if aqi_value is not None:
                try:
                    record["aqi"] = int(aqi_value)
                except (TypeError, ValueError):
                    record["aqi"] = None
        record["dominant_pollutant"] = _dominant_pollutant(pollutant_values)
        record["raw_payload"] = {
            "timestamp": timestamp,
            "variables": {
                key: series.get(key)[idx]
                for key in series.keys()
                if key != "time" and series.get(key) and idx < len(series.get(key))
            },
        }
        records.append(record)
    return records
