from dataclasses import dataclass
from typing import Dict, Iterable, List


@dataclass(frozen=True)
class City:
    name: str
    country: str
    latitude: float
    longitude: float


DEFAULT_CITIES: List[City] = [
    City("Paris", "France", 48.8566, 2.3522),
    City("London", "United Kingdom", 51.5074, -0.1278),
    City("Berlin", "Germany", 52.52, 13.405),
    City("Madrid", "Spain", 40.4168, -3.7038),
    City("Rome", "Italy", 41.9028, 12.4964),
    City("Amsterdam", "Netherlands", 52.3676, 4.9041),
    City("Copenhagen", "Denmark", 55.6761, 12.5683),
    City("Prague", "Czech Republic", 50.0755, 14.4378),
    City("Vienna", "Austria", 48.2082, 16.3738),
    City("Warsaw", "Poland", 52.2297, 21.0122),
]


def cities_from_overrides(overrides: Dict[str, Dict[str, float]]) -> Iterable[City]:
    """Build City instances from a dynamic mapping."""
    for name, data in overrides.items():
        yield City(
            name=name,
            country=data.get("country", ""),
            latitude=float(data["latitude"]),
            longitude=float(data["longitude"]),
        )
