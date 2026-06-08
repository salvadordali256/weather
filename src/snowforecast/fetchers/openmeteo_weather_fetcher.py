"""
Open-Meteo Weather Data Fetcher
Fetches historical weather data from Open-Meteo and stores locally
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OpenMeteoWeatherFetcher:
    """
    Fetches weather data from Open-Meteo Historical Weather API
    Historical data from 1940 onwards, hourly resolution
    No API key required, generous rate limits
    """
    
    def __init__(self, storage_path: str = "./weather_data"):
        self.base_url = "https://archive-api.open-meteo.com/v1/archive"
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        # Reasonable rate limiting (even though not strictly required)
        self.requests_per_second = 10
        self.last_request_time = 0
        self.min_request_interval = 1.0 / self.requests_per_second
    
    def _rate_limit(self):
        """Enforce conservative rate limiting"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def fetch_historical_data(self,
                            latitude: float,
                            longitude: float,
                            start_date: str,
                            end_date: str,
                            hourly_params: Optional[List[str]] = None,
                            daily_params: Optional[List[str]] = None,
                            timezone: str = "UTC") -> Dict[str, pd.DataFrame]:
        """
        Fetch historical weather data for a location
        
        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            hourly_params: List of hourly variables to fetch
            daily_params: List of daily variables to fetch
            timezone: Timezone (default: UTC)
        
        Returns:
            Dictionary with 'hourly' and 'daily' DataFrames
        
        Available hourly parameters:
            temperature_2m, relative_humidity_2m, dew_point_2m, apparent_temperature,
            pressure_msl, surface_pressure, precipitation, rain, snowfall, snow_depth,
            weather_code, cloud_cover, cloud_cover_low, cloud_cover_mid, cloud_cover_high,
            et0_fao_evapotranspiration, vapour_pressure_deficit, wind_speed_10m,
            wind_speed_100m, wind_direction_10m, wind_direction_100m, wind_gusts_10m,
            soil_temperature_0_to_7cm, soil_moisture_0_to_7cm
        
        Available daily parameters:
            temperature_2m_max, temperature_2m_min, temperature_2m_mean, apparent_temperature_max,
            apparent_temperature_min, apparent_temperature_mean, precipitation_sum, rain_sum,
            snowfall_sum, precipitation_hours, sunrise, sunset, daylight_duration,
            sunshine_duration, wind_speed_10m_max, wind_gusts_10m_max, wind_direction_10m_dominant,
            shortwave_radiation_sum, et0_fao_evapotranspiration
        """
        if hourly_params is None:
            hourly_params = [
                "temperature_2m",
                "relative_humidity_2m",
                "precipitation",
                "rain",
                "snowfall",
                "snow_depth",
                "wind_speed_10m",
                "wind_direction_10m",
                "pressure_msl"
            ]
        
        if daily_params is None:
            daily_params = [
                "temperature_2m_max",
                "temperature_2m_min",
                "temperature_2m_mean",
                "precipitation_sum",
                "rain_sum",
                "snowfall_sum",
                "wind_speed_10m_max",
                "wind_gusts_10m_max",
                "sunshine_duration"
            ]
        
        self._rate_limit()
        
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "timezone": timezone
        }
        
        if hourly_params:
            params["hourly"] = ",".join(hourly_params)
        
        if daily_params:
            params["daily"] = ",".join(daily_params)
        
        try:
            response = requests.get(self.base_url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            result = {}
            
            # Process hourly data
            if "hourly" in data and hourly_params:
                hourly_df = pd.DataFrame(data["hourly"])
                hourly_df["time"] = pd.to_datetime(hourly_df["time"])
                result["hourly"] = hourly_df
                logger.info(f"Fetched {len(hourly_df)} hourly records")
            
            # Process daily data
            if "daily" in data and daily_params:
                daily_df = pd.DataFrame(data["daily"])
                daily_df["time"] = pd.to_datetime(daily_df["time"])
                result["daily"] = daily_df
                logger.info(f"Fetched {len(daily_df)} daily records")
            
            # Add location metadata
            if "latitude" in data:
                result["metadata"] = {
                    "latitude": data.get("latitude"),
                    "longitude": data.get("longitude"),
                    "elevation": data.get("elevation"),
                    "timezone": data.get("timezone"),
                    "timezone_abbreviation": data.get("timezone_abbreviation")
                }
            
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return {}
    
    def fetch_date_range_chunked(self,
                                latitude: float,
                                longitude: float,
                                start_date: str,
                                end_date: str,
                                chunk_years: int = 5,
                                hourly_params: Optional[List[str]] = None,
                                daily_params: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:
        """
        Fetch large date ranges by chunking into smaller requests
        
        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            chunk_years: Number of years per chunk (default: 5)
            hourly_params: List of hourly variables
            daily_params: List of daily variables
        
        Returns:
            Dictionary with combined 'hourly' and 'daily' DataFrames
        """
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        hourly_chunks = []
        daily_chunks = []
        metadata = None
        
        current = start
        
        while current < end:
            chunk_end = min(current + timedelta(days=365 * chunk_years), end)
            
            logger.info(f"Fetching {current.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}")
            
            chunk_data = self.fetch_historical_data(
                latitude,
                longitude,
                current.strftime("%Y-%m-%d"),
                chunk_end.strftime("%Y-%m-%d"),
                hourly_params,
                daily_params
            )
            
            if "hourly" in chunk_data:
                hourly_chunks.append(chunk_data["hourly"])
            
            if "daily" in chunk_data:
                daily_chunks.append(chunk_data["daily"])
            
            if not metadata and "metadata" in chunk_data:
                metadata = chunk_data["metadata"]
            
            current = chunk_end + timedelta(days=1)
        
        result = {}
        
        if hourly_chunks:
            result["hourly"] = pd.concat(hourly_chunks, ignore_index=True)
            logger.info(f"Combined {len(result['hourly'])} hourly records")
        
        if daily_chunks:
            result["daily"] = pd.concat(daily_chunks, ignore_index=True)
            logger.info(f"Combined {len(result['daily'])} daily records")
        
        if metadata:
            result["metadata"] = metadata
        
        return result
    
    def fetch_multiple_locations(self,
                                locations: List[Tuple[str, float, float]],
                                start_date: str,
                                end_date: str,
                                max_workers: int = 5) -> Dict[str, Dict]:
        """
        Fetch data for multiple locations in parallel
        
        Args:
            locations: List of (name, latitude, longitude) tuples
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            max_workers: Maximum parallel workers
        
        Returns:
            Dictionary mapping location names to their data
        """
        results = {}
        
        def fetch_location(location_info):
            name, lat, lon = location_info
            logger.info(f"Fetching data for {name} ({lat}, {lon})")
            data = self.fetch_date_range_chunked(lat, lon, start_date, end_date)
            return name, data
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_location, loc): loc for loc in locations}
            
            for future in as_completed(futures):
                try:
                    name, data = future.result()
                    results[name] = data
                except Exception as e:
                    logger.error(f"Error fetching location: {e}")
        
        return results
    
    def create_grid_points(self,
                          north: float,
                          south: float,
                          east: float,
                          west: float,
                          resolution: float = 0.25) -> List[Tuple[float, float]]:
        """
        Create a grid of coordinate points for a bounding box
        
        Args:
            north: Northern latitude boundary
            south: Southern latitude boundary
            east: Eastern longitude boundary
            west: Western longitude boundary
            resolution: Grid resolution in degrees (default: 0.25)
        
        Returns:
            List of (latitude, longitude) tuples
        """
        lats = np.arange(south, north + resolution, resolution)
        lons = np.arange(west, east + resolution, resolution)
        
        grid_points = [(lat, lon) for lat in lats for lon in lons]
        
        logger.info(f"Created grid with {len(grid_points)} points")
        return grid_points
    
    def save_to_storage(self,
                       data: Dict[str, pd.DataFrame],
                       location_name: str,
                       format: str = "parquet",
                       partition_by: str = "year"):
        """
        Save data to local storage with efficient formats
        
        Args:
            data: Dictionary with 'hourly' and/or 'daily' DataFrames
            location_name: Location identifier for file naming
            format: File format (parquet, csv, or feather)
            partition_by: Partition strategy (year, month, or none)
        """
        location_clean = location_name.replace(" ", "_").replace(",", "")
        location_path = self.storage_path / location_clean
        location_path.mkdir(exist_ok=True)
        
        # Save metadata if present
        if "metadata" in data:
            metadata_file = location_path / "metadata.json"
            import json
            with open(metadata_file, 'w') as f:
                json.dump(data["metadata"], f, indent=2)
            logger.info(f"Saved metadata to {metadata_file}")
        
        # Save hourly data
        if "hourly" in data and not data["hourly"].empty:
            df = data["hourly"].copy()
            df['year'] = df['time'].dt.year
            df['month'] = df['time'].dt.month
            
            self._save_dataframe(
                df,
                location_path / "hourly",
                f"{location_clean}_hourly",
                format,
                partition_by
            )
        
        # Save daily data
        if "daily" in data and not data["daily"].empty:
            df = data["daily"].copy()
            df['year'] = df['time'].dt.year
            df['month'] = df['time'].dt.month
            
            self._save_dataframe(
                df,
                location_path / "daily",
                f"{location_clean}_daily",
                format,
                partition_by
            )
    
    def _save_dataframe(self,
                       df: pd.DataFrame,
                       base_path: Path,
                       base_filename: str,
                       format: str,
                       partition_by: str):
        """Save DataFrame with partitioning"""
        base_path.mkdir(exist_ok=True)
        
        if partition_by == "none":
            filename = f"{base_filename}_all.{format}"
            filepath = base_path / filename
            self._save_file(df, filepath, format)
            
        elif partition_by == "year":
            for year, year_df in df.groupby('year'):
                filename = f"{base_filename}_{year}.{format}"
                filepath = base_path / str(year)
                filepath.mkdir(exist_ok=True)
                self._save_file(year_df, filepath / filename, format)
                
        elif partition_by == "month":
            for (year, month), month_df in df.groupby(['year', 'month']):
                filename = f"{base_filename}_{year}_{month:02d}.{format}"
                filepath = base_path / str(year)
                filepath.mkdir(exist_ok=True)
                self._save_file(month_df, filepath / filename, format)
    
    def _save_file(self, df: pd.DataFrame, filepath: Path, format: str):
        """Save DataFrame in specified format"""
        try:
            if format == "parquet":
                df.to_parquet(filepath, engine='pyarrow', compression='snappy')
            elif format == "feather":
                df.to_feather(filepath)
            elif format == "csv":
                df.to_csv(filepath, index=False, compression='gzip')
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            file_size = filepath.stat().st_size / (1024 * 1024)  # MB
            logger.info(f"Saved {len(df)} records to {filepath} ({file_size:.2f} MB)")
            
        except Exception as e:
            logger.error(f"Error saving file: {e}")
    
    def load_from_storage(self,
                         location_name: str,
                         data_type: str = "daily",
                         year: Optional[int] = None) -> pd.DataFrame:
        """
        Load data from storage
        
        Args:
            location_name: Location identifier
            data_type: 'hourly' or 'daily'
            year: Specific year to load (None for all)
        
        Returns:
            DataFrame with weather data
        """
        location_clean = location_name.replace(" ", "_").replace(",", "")
        location_path = self.storage_path / location_clean / data_type
        
        if not location_path.exists():
            logger.warning(f"Path does not exist: {location_path}")
            return pd.DataFrame()
        
        dfs = []
        
        if year:
            # Load specific year
            year_path = location_path / str(year)
            if year_path.exists():
                for file in year_path.glob("*.parquet"):
                    dfs.append(pd.read_parquet(file))
        else:
            # Load all years
            for file in location_path.rglob("*.parquet"):
                dfs.append(pd.read_parquet(file))
        
        if dfs:
            combined = pd.concat(dfs, ignore_index=True)
            logger.info(f"Loaded {len(combined)} records from storage")
            return combined
        
        return pd.DataFrame()

    def fetch_current_weather(self,
                             latitude: float,
                             longitude: float,
                             current_params: Optional[List[str]] = None,
                             timezone: str = "UTC") -> Dict:
        """
        Fetch current weather conditions for a location

        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            current_params: List of current weather variables to fetch
            timezone: Timezone (default: UTC)

        Returns:
            Dictionary with 'current' weather data

        Available current parameters:
            temperature_2m, relative_humidity_2m, apparent_temperature, is_day,
            precipitation, rain, showers, snowfall, weather_code, cloud_cover,
            pressure_msl, surface_pressure, wind_speed_10m, wind_direction_10m,
            wind_gusts_10m
        """
        if current_params is None:
            current_params = [
                "temperature_2m",
                "relative_humidity_2m",
                "apparent_temperature",
                "precipitation",
                "rain",
                "snowfall",
                "wind_speed_10m",
                "wind_direction_10m"
            ]

        self._rate_limit()

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "current": ",".join(current_params),
            "timezone": timezone
        }

        try:
            # Use forecast API for current weather
            response = requests.get(
                "https://api.open-meteo.com/v1/forecast",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            data = response.json()

            result = {}

            # Extract current weather data
            if "current" in data:
                result["current"] = data["current"]
                logger.info("Fetched current weather")

            # Add location metadata
            if "latitude" in data:
                result["metadata"] = {
                    "latitude": data.get("latitude"),
                    "longitude": data.get("longitude"),
                    "elevation": data.get("elevation"),
                    "timezone": data.get("timezone"),
                    "timezone_abbreviation": data.get("timezone_abbreviation")
                }

            return result

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return {}

    def fetch_forecast(self,
                      latitude: float,
                      longitude: float,
                      forecast_days: int = 7,
                      hourly_params: Optional[List[str]] = None,
                      daily_params: Optional[List[str]] = None,
                      timezone: str = "UTC") -> Dict[str, pd.DataFrame]:
        """
        Fetch weather forecast for a location

        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            forecast_days: Number of days to forecast (1-16, default: 7)
            hourly_params: List of hourly forecast variables
            daily_params: List of daily forecast variables
            timezone: Timezone (default: UTC)

        Returns:
            Dictionary with 'hourly' and/or 'daily' forecast DataFrames

        Available hourly parameters:
            temperature_2m, relative_humidity_2m, dew_point_2m, apparent_temperature,
            precipitation_probability, precipitation, rain, showers, snowfall, snow_depth,
            weather_code, pressure_msl, surface_pressure, cloud_cover, cloud_cover_low,
            cloud_cover_mid, cloud_cover_high, visibility, evapotranspiration,
            et0_fao_evapotranspiration, vapour_pressure_deficit, wind_speed_10m,
            wind_speed_80m, wind_speed_120m, wind_speed_180m, wind_direction_10m,
            wind_direction_80m, wind_direction_120m, wind_direction_180m, wind_gusts_10m,
            temperature_80m, temperature_120m, temperature_180m, soil_temperature_0cm,
            soil_temperature_6cm, soil_temperature_18cm, soil_temperature_54cm,
            soil_moisture_0_1cm, soil_moisture_1_3cm, soil_moisture_3_9cm,
            soil_moisture_9_27cm, soil_moisture_27_81cm

        Available daily parameters:
            weather_code, temperature_2m_max, temperature_2m_min, apparent_temperature_max,
            apparent_temperature_min, sunrise, sunset, daylight_duration, sunshine_duration,
            uv_index_max, uv_index_clear_sky_max, precipitation_sum, rain_sum, showers_sum,
            snowfall_sum, precipitation_hours, precipitation_probability_max,
            precipitation_probability_min, precipitation_probability_mean, wind_speed_10m_max,
            wind_gusts_10m_max, wind_direction_10m_dominant, shortwave_radiation_sum,
            et0_fao_evapotranspiration
        """
        if forecast_days < 1 or forecast_days > 16:
            forecast_days = 7

        if daily_params is None and hourly_params is None:
            daily_params = [
                "temperature_2m_max",
                "temperature_2m_min",
                "precipitation_sum",
                "rain_sum",
                "snowfall_sum",
                "wind_speed_10m_max"
            ]

        self._rate_limit()

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "forecast_days": forecast_days,
            "timezone": timezone
        }

        if hourly_params:
            params["hourly"] = ",".join(hourly_params)

        if daily_params:
            params["daily"] = ",".join(daily_params)

        try:
            response = requests.get(
                "https://api.open-meteo.com/v1/forecast",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            data = response.json()

            result = {}

            # Process hourly data
            if "hourly" in data:
                hourly_df = pd.DataFrame(data["hourly"])
                result["hourly"] = hourly_df
                logger.info(f"Fetched {len(hourly_df)} hourly forecast records")

            # Process daily data
            if "daily" in data:
                daily_df = pd.DataFrame(data["daily"])
                result["daily"] = daily_df
                logger.info(f"Fetched {len(daily_df)} daily forecast records")

            # Add location metadata
            if "latitude" in data:
                result["metadata"] = {
                    "latitude": data.get("latitude"),
                    "longitude": data.get("longitude"),
                    "elevation": data.get("elevation"),
                    "timezone": data.get("timezone"),
                    "timezone_abbreviation": data.get("timezone_abbreviation")
                }

            return result

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return {}


def main():
    """Example usage"""
    
    STORAGE_PATH = "/path/to/your/massive/drive/weather_data/openmeteo"
    
    fetcher = OpenMeteoWeatherFetcher(STORAGE_PATH)
    
    # Example 1: Fetch data for a single location (New York City)
    logger.info("Fetching data for New York City...")
    nyc_data = fetcher.fetch_date_range_chunked(
        latitude=40.7128,
        longitude=-74.0060,
        start_date="1940-01-01",
        end_date="2024-12-31",
        chunk_years=10
    )
    
    if nyc_data:
        fetcher.save_to_storage(
            nyc_data,
            "New_York_City",
            format="parquet",
            partition_by="year"
        )
    
    # Example 2: Fetch data for multiple US cities
    us_cities = [
        ("New_York_NY", 40.7128, -74.0060),
        ("Los_Angeles_CA", 34.0522, -118.2437),
        ("Chicago_IL", 41.8781, -87.6298),
        ("Houston_TX", 29.7604, -95.3698),
        ("Phoenix_AZ", 33.4484, -112.0740),
        ("Philadelphia_PA", 39.9526, -75.1652),
        ("San_Antonio_TX", 29.4241, -98.4936),
        ("San_Diego_CA", 32.7157, -117.1611),
        ("Dallas_TX", 32.7767, -96.7970),
        ("San_Jose_CA", 37.3382, -121.8863)
    ]
    
    logger.info("Fetching data for multiple cities...")
    all_city_data = fetcher.fetch_multiple_locations(
        us_cities,
        start_date="2000-01-01",
        end_date="2024-12-31",
        max_workers=5
    )
    
    for city_name, city_data in all_city_data.items():
        fetcher.save_to_storage(city_data, city_name, format="parquet")
    
    # Example 3: Create a grid for a region (e.g., California)
    logger.info("Creating grid for California...")
    ca_grid = fetcher.create_grid_points(
        north=42.0,   # Northern California
        south=32.5,   # Southern California
        east=-114.1,  # Eastern border
        west=-124.4,  # Western border
        resolution=0.5  # ~55km spacing
    )
    
    # Fetch data for first 10 grid points as example
    for i, (lat, lon) in enumerate(ca_grid[:10]):
        logger.info(f"Fetching grid point {i+1}/10")
        grid_data = fetcher.fetch_historical_data(
            latitude=lat,
            longitude=lon,
            start_date="2020-01-01",
            end_date="2024-12-31",
            daily_params=["temperature_2m_mean", "precipitation_sum"]
        )
        
        if grid_data:
            location_name = f"CA_Grid_{lat:.2f}_{lon:.2f}"
            fetcher.save_to_storage(grid_data, location_name, format="parquet")
    
    # Example 4: Load data back from storage
    logger.info("Loading data from storage...")
    loaded_data = fetcher.load_from_storage("New_York_City", "daily", year=2023)
    
    if not loaded_data.empty:
        print("\nSample of loaded data:")
        print(loaded_data.head())
        print(f"\nTotal records: {len(loaded_data)}")
        print(f"\nColumns: {list(loaded_data.columns)}")


if __name__ == "__main__":
    main()
