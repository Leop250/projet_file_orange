import argparse
import json
import logging
from pathlib import Path

from air_quality.cities import DEFAULT_CITIES, City, cities_from_overrides
from air_quality.config import MissingConfiguration, load_settings
from air_quality.orchestrator import ingest_air_quality_data
from air_quality.storage import AirQualityStorage
from air_quality.visualization import generate_visualizations

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
LOGGER = logging.getLogger(__name__)


def _load_city_overrides(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handler:
        if path.endswith(".json"):
            return json.load(handler)
        raise ValueError("Only JSON files are supported for city overrides.")


def _resolve_cities(args) -> list[City]:
    cities = list(DEFAULT_CITIES)
    if args.cities:
        name_set = {name.strip().lower() for name in args.cities}
        cities = [city for city in cities if city.name.lower() in name_set]
    if args.city_overrides:
        overrides = _load_city_overrides(args.city_overrides)
        cities.extend(list(cities_from_overrides(overrides)))
    if not cities:
        raise ValueError("No cities selected. Check --cities or override file.")
    return cities


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ingest and visualize air quality data for European cities.",
    )
    parser.add_argument(
        "--cities",
        nargs="+",
        help="List of city names to ingest (defaults to predefined European cities).",
    )
    parser.add_argument(
        "--city-overrides",
        help=(
            "Path to a JSON file defining additional cities with lat/long metadata. "
            "Format: {\"City\": {\"country\": \"...\", \"latitude\": 0, \"longitude\": 0}}"
        ),
    )
    parser.add_argument(
        "--skip-ingest",
        action="store_true",
        help="Skip data ingestion step and only generate visualizations.",
    )
    parser.add_argument(
        "--skip-visuals",
        action="store_true",
        help="Skip visualization generation (ingestion only).",
    )
    parser.add_argument(
        "--skip-forecast",
        action="store_true",
        help="Skip Open-Meteo forecast ingestion (OpenAQ observations only).",
    )
    parser.add_argument(
        "--forecast-hours",
        type=int,
        default=72,
        help="Forecast horizon in hours when pulling Open-Meteo data (default: 72).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set logging verbosity.",
    )
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()
    logging.getLogger().setLevel(args.log_level)
    try:
        settings = load_settings()
    except MissingConfiguration as error:
        parser.error(str(error))
        return

    storage = AirQualityStorage(settings.database_path)
    selected_cities = _resolve_cities(args)
    LOGGER.info(
        "Working with %d cities. Database: %s",
        len(selected_cities),
        settings.database_path,
    )

    if not args.skip_ingest:
        result = ingest_air_quality_data(
            api_key=settings.api_key,
            storage=storage,
            cities=selected_cities,
            forecast_hours=args.forecast_hours,
            include_forecast=not args.skip_forecast,
        )
        LOGGER.info("Ingestion complete. Success: %s", ", ".join(result.successes) or "None")
        if result.failures:
            LOGGER.error("Failures encountered: %s", "; ".join(result.failures))

    if not args.skip_visuals:
        generate_visualizations(
            storage,
            output_dir=settings.artifacts_dir,
            focus_cities=[city.name for city in selected_cities],
        )
        LOGGER.info("Charts available in %s", Path(settings.artifacts_dir).resolve())


if __name__ == "__main__":
    main()
