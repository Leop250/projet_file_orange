import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, List, Optional


class AirQualityStorage:
    """SQLite-backed storage for air quality measurements."""

    def __init__(self, database_path: str) -> None:
        self._database_path = Path(database_path)
        if self._database_path.parent and not self._database_path.parent.exists():
            self._database_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize_schema()

    @contextmanager
    def _connect(self):
        connection = sqlite3.connect(self._database_path)
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def _initialize_schema(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS air_quality_measurements (
                    city TEXT NOT NULL,
                    country TEXT,
                    measured_at TEXT NOT NULL,
                    source TEXT NOT NULL,
                    aqi INTEGER,
                    category TEXT,
                    dominant_pollutant TEXT,
                    pm25 REAL,
                    pm10 REAL,
                    o3 REAL,
                    no2 REAL,
                    so2 REAL,
                    co REAL,
                    health_recommendations TEXT,
                    raw_payload TEXT,
                    received_at TEXT NOT NULL,
                    PRIMARY KEY (city, measured_at, source)
                )
                """
            )

    def upsert_measurements(self, records: Iterable[dict]) -> None:
        with self._connect() as connection:
            connection.executemany(
                """
                INSERT INTO air_quality_measurements (
                    city,
                    country,
                    measured_at,
                    source,
                    aqi,
                    category,
                    dominant_pollutant,
                    pm25,
                    pm10,
                    o3,
                    no2,
                    so2,
                    co,
                    health_recommendations,
                    raw_payload,
                    received_at
                )
                VALUES (
                    :city,
                    :country,
                    :measured_at,
                    :source,
                    :aqi,
                    :category,
                    :dominant_pollutant,
                    :pm25,
                    :pm10,
                    :o3,
                    :no2,
                    :so2,
                    :co,
                    :health_recommendations,
                    :raw_payload,
                    :received_at
                )
                ON CONFLICT(city, measured_at, source) DO UPDATE SET
                    aqi=excluded.aqi,
                    category=excluded.category,
                    dominant_pollutant=excluded.dominant_pollutant,
                    pm25=excluded.pm25,
                    pm10=excluded.pm10,
                    o3=excluded.o3,
                    no2=excluded.no2,
                    so2=excluded.so2,
                    co=excluded.co,
                    health_recommendations=excluded.health_recommendations,
                    raw_payload=excluded.raw_payload,
                    received_at=excluded.received_at
                """,
                [
                    {
                        **record,
                        "raw_payload": json.dumps(record.get("raw_payload", {})),
                    }
                    for record in records
                ],
            )

    def fetch_measurements(
        self,
        *,
        cities: Optional[List[str]] = None,
        min_datetime: Optional[str] = None,
    ):
        filters = []
        params: List = []
        if cities:
            placeholders = ",".join("?" for _ in cities)
            filters.append(f"city IN ({placeholders})")
            params.extend(cities)
        if min_datetime:
            filters.append("measured_at >= ?")
            params.append(min_datetime)
        clause = f"WHERE {' AND '.join(filters)}" if filters else ""
        query = f"""
            SELECT city,
                   country,
                   measured_at,
                   source,
                   aqi,
                   category,
                   dominant_pollutant,
                   pm25,
                   pm10,
                   o3,
                   no2,
                   so2,
                   co,
                   health_recommendations,
                   received_at
            FROM air_quality_measurements
            {clause}
            ORDER BY measured_at ASC
        """
        with self._connect() as connection:
            cursor = connection.execute(query, params)
            columns = [col[0] for col in cursor.description]
            for row in cursor.fetchall():
                yield dict(zip(columns, row))
