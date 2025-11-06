from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, Optional

import matplotlib
import pandas as pd

from .storage import AirQualityStorage

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402  pylint: disable=wrong-import-position

LOGGER = logging.getLogger(__name__)


def _prepare_dataframe(records: Iterable[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(records)
    if frame.empty:
        return frame
    frame["measured_at"] = pd.to_datetime(frame["measured_at"], utc=True)
    frame = frame.sort_values("measured_at")
    return frame


def plot_city_pollutant_timeseries(
    data: pd.DataFrame,
    city: str,
    *,
    output_dir: Path,
) -> None:
    city_data = data[data["city"] == city]
    pollutant_columns = [
        col
        for col in ["pm25", "pm10", "o3", "no2", "so2", "co"]
        if col in city_data.columns and city_data[col].notna().any()
    ]
    if city_data.empty or not pollutant_columns:
        LOGGER.warning("No pollutant data available for %s, skipping chart", city)
        return
    plt.figure(figsize=(11, 6))
    for column in pollutant_columns:
        plt.plot(
            city_data["measured_at"],
            city_data[column],
            marker="o",
            label=column.upper(),
        )
    plt.title(f"Pollutant concentration trend — {city}")
    plt.xlabel("Date")
    plt.ylabel("Concentration (reported units)")
    plt.legend()
    plt.grid(True, linestyle="--", alpha=0.4)
    output_path = output_dir / f"{city.lower().replace(' ', '_')}_pollutants.png"
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()
    LOGGER.info("Saved pollutant chart for %s to %s", city, output_path)


def plot_pollutant_summary(
    data: pd.DataFrame,
    *,
    output_dir: Path,
) -> None:
    pollutant_columns = ["pm25", "pm10", "o3", "no2", "so2", "co"]
    available = [col for col in pollutant_columns if col in data.columns]
    if not available:
        LOGGER.warning("No pollutant concentration data available for summary chart")
        return
    recent = data.dropna(subset=available).copy()
    if recent.empty:
        return
    recent["measured_at"] = pd.to_datetime(recent["measured_at"])
    last_week = recent[recent["measured_at"] >= (recent["measured_at"].max() - pd.Timedelta(days=7))]
    if last_week.empty:
        last_week = recent
    pivot = last_week.groupby("city")[available].mean().sort_index()
    if pivot.empty:
        return
    pivot.plot(kind="bar", figsize=(12, 6))
    plt.title("Average pollutant concentration (last 7 days where available)")
    plt.ylabel("µg/m³ or ppm (depending on pollutant)")
    plt.xlabel("City")
    plt.grid(axis="y", linestyle="--", alpha=0.5)
    plt.tight_layout()
    output_path = output_dir / "pollutant_summary.png"
    plt.savefig(output_path)
    plt.close()
    LOGGER.info("Saved pollutant summary chart to %s", output_path)


def generate_visualizations(
    storage: AirQualityStorage,
    *,
    output_dir: str,
    focus_cities: Optional[Iterable[str]] = None,
) -> None:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    records = list(
        storage.fetch_measurements(
            cities=list(focus_cities) if focus_cities else None,
        )
    )
    frame = _prepare_dataframe(records)
    if frame.empty:
        LOGGER.warning("No data available yet. Run the ingestion pipeline first.")
        return
    for city in sorted(frame["city"].unique()):
        plot_city_pollutant_timeseries(frame, city, output_dir=output_path)
    plot_pollutant_summary(frame, output_dir=output_path)
