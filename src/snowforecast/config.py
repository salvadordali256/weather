"""
Configuration Management
========================

Handles loading configuration from environment variables and .env files.
"""

import os
from pathlib import Path
from typing import Optional
from dataclasses import dataclass
from datetime import datetime


def load_env_file(env_path: str = ".env"):
    """Load environment variables from .env file"""
    env_file = Path(env_path)

    if not env_file.exists():
        return False

    with open(env_file, 'r') as f:
        for line in f:
            line = line.strip()

            # Skip comments and empty lines
            if not line or line.startswith('#'):
                continue

            # Parse KEY=VALUE
            if '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()

                # Only set if not already in environment
                if key not in os.environ:
                    os.environ[key] = value

    return True


@dataclass
class WeatherConfig:
    """Weather data fetcher configuration"""

    # API Configuration
    noaa_api_token: str

    # Storage Configuration
    storage_path: str
    storage_format: str = "parquet"
    partition_by: str = "year"

    # Data Source Configuration
    use_noaa: bool = True
    use_openmeteo: bool = True

    # Download Configuration
    start_date: str = "2000-01-01"
    end_date: Optional[str] = None
    max_stations_per_state: int = 50
    grid_resolution: float = 0.5

    # Performance Configuration
    max_workers: int = 5
    chunk_days: int = 365
    chunk_years: int = 5

    # Logging Configuration
    log_level: str = "INFO"
    log_file: Optional[str] = "weather_orchestrator.log"

    @classmethod
    def from_env(cls, env_file: str = ".env") -> "WeatherConfig":
        """Load configuration from environment variables"""

        # Try to load .env file
        load_env_file(env_file)

        # Get NOAA API token
        noaa_token = os.getenv("NOAA_API_TOKEN", "")
        if not noaa_token or "YOUR_" in noaa_token:
            raise ValueError(
                "NOAA_API_TOKEN not set! "
                "Get your free token at: https://www.ncdc.noaa.gov/cdo-web/token\n"
                "Then set it in your .env file or environment"
            )

        # Get storage path
        storage_path = os.getenv("STORAGE_PATH", "./weather_data")

        # Get end date (default to today)
        end_date = os.getenv("END_DATE")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        # Build configuration
        config = cls(
            noaa_api_token=noaa_token,
            storage_path=storage_path,
            storage_format=os.getenv("STORAGE_FORMAT", "parquet"),
            partition_by=os.getenv("PARTITION_BY", "year"),
            use_noaa=os.getenv("USE_NOAA", "true").lower() == "true",
            use_openmeteo=os.getenv("USE_OPENMETEO", "true").lower() == "true",
            start_date=os.getenv("START_DATE", "2000-01-01"),
            end_date=end_date,
            max_stations_per_state=int(os.getenv("MAX_STATIONS_PER_STATE", "50")),
            grid_resolution=float(os.getenv("GRID_RESOLUTION", "0.5")),
            max_workers=int(os.getenv("MAX_WORKERS", "5")),
            chunk_days=int(os.getenv("CHUNK_DAYS", "365")),
            chunk_years=int(os.getenv("CHUNK_YEARS", "5")),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            log_file=os.getenv("LOG_FILE", "weather_orchestrator.log")
        )

        return config

    def validate(self) -> bool:
        """Validate configuration"""
        errors = []

        # Validate dates
        try:
            datetime.strptime(self.start_date, "%Y-%m-%d")
            datetime.strptime(self.end_date, "%Y-%m-%d")
        except ValueError as e:
            errors.append(f"Invalid date format: {e}")

        # Validate storage format
        if self.storage_format not in ["parquet", "csv", "feather"]:
            errors.append(f"Invalid storage format: {self.storage_format}")

        # Validate partition strategy
        if self.partition_by not in ["year", "month", "none"]:
            errors.append(f"Invalid partition strategy: {self.partition_by}")

        # Validate at least one source enabled
        if not self.use_noaa and not self.use_openmeteo:
            errors.append("At least one data source must be enabled")

        if errors:
            raise ValueError("Configuration validation failed:\n  " + "\n  ".join(errors))

        return True

    def __str__(self) -> str:
        """String representation"""
        return f"""
Weather Data Fetcher Configuration
{'='*80}
API:
  NOAA Token:        {'***' + self.noaa_api_token[-6:] if len(self.noaa_api_token) > 6 else '***'}

Storage:
  Path:              {self.storage_path}
  Format:            {self.storage_format}
  Partition By:      {self.partition_by}

Data Sources:
  NOAA:              {'Enabled' if self.use_noaa else 'Disabled'}
  Open-Meteo:        {'Enabled' if self.use_openmeteo else 'Disabled'}

Date Range:
  Start:             {self.start_date}
  End:               {self.end_date}

Performance:
  Max Workers:       {self.max_workers}
  Chunk Days:        {self.chunk_days}
  Chunk Years:       {self.chunk_years}

Logging:
  Level:             {self.log_level}
  File:              {self.log_file or 'Console only'}
{'='*80}
"""


def main():
    """Test configuration loading"""
    try:
        config = WeatherConfig.from_env()
        config.validate()
        print(config)
    except ValueError as e:
        print(f"❌ Configuration Error: {e}")
        print()
        print("💡 Make sure you:")
        print("  1. Copy .env.example to .env")
        print("  2. Get NOAA API token from: https://www.ncdc.noaa.gov/cdo-web/token")
        print("  3. Set NOAA_API_TOKEN in .env file")


if __name__ == "__main__":
    main()
