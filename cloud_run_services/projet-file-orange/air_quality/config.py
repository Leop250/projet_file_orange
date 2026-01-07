import os
from dataclasses import dataclass


class MissingConfiguration(Exception):
    """Raised when a required configuration value cannot be loaded."""


@dataclass(frozen=True)
class Settings:
    api_key: str | None
    database_path: str
    artifacts_dir: str


def load_settings() -> Settings:
    """Load runtime configuration from environment variables."""
    api_key = os.getenv("OPENAQ_API_KEY")
    if not api_key:
        raise MissingConfiguration(
            "Missing OpenAQ API key. Set OPENAQ_API_KEY in your environment "
            "with the value provided by https://docs.openaq.org/."
        )

    database_path = os.getenv(
        "AIR_QUALITY_DB_PATH",
        os.path.join(os.getcwd(), "data", "air_quality.db"),
    )

    artifacts_dir = os.getenv(
        "AIR_QUALITY_ARTIFACTS_DIR",
        os.path.join(os.getcwd(), "artifacts"),
    )

    return Settings(
        api_key=api_key,
        database_path=database_path,
        artifacts_dir=artifacts_dir,
    )
