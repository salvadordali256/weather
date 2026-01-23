"""
NOAA NCEI Weather Data Fetcher
Fetches historical weather data from NOAA and stores locally
"""

import requests
import pandas as pd
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_exponential

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NOAAWeatherFetcher:
    """
    Fetches weather data from NOAA NCEI API
    Rate limits: 5 requests/second, 10,000 requests/day per token
    """
    
    def __init__(
        self,
        api_token: Optional[str],
        storage_path: str = "./weather_data",
        user_agent: Optional[str] = None,
    ):
        if not user_agent:
            raise ValueError("NOAA API requires a User-Agent string (e.g., '(site, email)').")

        self.api_token = api_token
        self.user_agent = user_agent
        self.base_url = "https://www.ncei.noaa.gov/cdo-web/api/v2"
        self.data_service_url = "https://www.ncei.noaa.gov/access/services/data/v1"
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        # Rate limiting: 5 requests per second
        self.requests_per_second = 5
        self.last_request_time = 0
        self.min_request_interval = 1.0 / self.requests_per_second
        
        # Request counter for daily limit tracking
        self.daily_request_count = 0
        self.daily_limit = 10000
        
    def _rate_limit(self):
        """Enforce rate limiting"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
        self.daily_request_count += 1
        
        if self.daily_request_count >= self.daily_limit:
            logger.warning("Daily request limit reached!")
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def _make_request(self, url: str, params: Dict) -> Dict:
        """Make API request with retry logic"""
        self._rate_limit()
        
        headers = {"User-Agent": self.user_agent}
        if self.api_token:
            headers["token"] = self.api_token
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            return response.json() if response.content else {}
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
    
    def find_stations(self, 
                     locationid: Optional[str] = None,
                     datasetid: str = "GHCND",
                     datatypeid: Optional[List[str]] = None,
                     extent: Optional[tuple] = None,
                     limit: int = 1000) -> pd.DataFrame:
        """
        Find weather stations matching criteria
        
        Args:
            locationid: Location ID (e.g., "FIPS:37" for North Carolina, "ZIP:28801")
            datasetid: Dataset ID (default: GHCND - Global Historical Climatology Network Daily)
            datatypeid: List of data types (e.g., ["TMAX", "TMIN", "PRCP"])
            extent: Bounding box (north, south, east, west) - e.g., (40.91, 40.50, -73.70, -74.25)
            limit: Maximum number of stations to return
        
        Returns:
            DataFrame with station information
        """
        url = f"{self.base_url}/stations"
        
        params = {
            "datasetid": datasetid,
            "limit": limit
        }
        
        if locationid:
            params["locationid"] = locationid

        if datatypeid:
            # NOAA API doesn't support multiple datatypeids in find_stations
            # Use the first one, or remove this filter if needed
            params["datatypeid"] = datatypeid[0] if isinstance(datatypeid, list) else datatypeid
        
        if extent:
            params["extent"] = f"{extent[0]},{extent[1]},{extent[2]},{extent[3]}"
        
        try:
            data = self._make_request(url, params)
            
            if "results" in data:
                stations_df = pd.DataFrame(data["results"])
                
                # Save stations to file
                stations_file = self.storage_path / f"stations_{datasetid}_{datetime.now().strftime('%Y%m%d')}.csv"
                stations_df.to_csv(stations_file, index=False)
                logger.info(f"Found {len(stations_df)} stations, saved to {stations_file}")
                
                return stations_df
            else:
                logger.warning("No stations found")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error finding stations: {e}")
            return pd.DataFrame()
    
    def fetch_daily_data(self,
                        stationid: str,
                        start_date: str,
                        end_date: str,
                        datatypes: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Fetch daily weather data for a station
        
        Args:
            stationid: Station ID (e.g., "GHCND:USC00084731")
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            datatypes: List of data types to fetch (default: TMAX, TMIN, PRCP)
        
        Returns:
            DataFrame with daily weather data
        """
        if datatypes is None:
            datatypes = ["TMAX", "TMIN", "PRCP", "SNOW", "SNWD", "AWND"]
        
        url = f"{self.base_url}/data"
        
        params = {
            "datasetid": "GHCND",
            "stationid": stationid,
            "startdate": start_date,
            "enddate": end_date,
            "units": "metric",
            "limit": 1000  # Max per request
        }
        
        all_data = []
        offset = 1
        
        while True:
            params["offset"] = offset
            
            try:
                response = self._make_request(url, params)
                
                if "results" not in response:
                    break
                
                results = response["results"]
                
                # Filter by requested data types
                filtered_results = [r for r in results if r.get("datatype") in datatypes]
                all_data.extend(filtered_results)
                
                logger.info(f"Fetched {len(filtered_results)} records (offset: {offset})")
                
                # Check if there are more results
                if len(results) < 1000:
                    break
                
                offset += 1000
                
            except Exception as e:
                logger.error(f"Error fetching data: {e}")
                break
        
        if all_data:
            df = pd.DataFrame(all_data)
            
            # Pivot data so each datatype is a column
            df_pivot = df.pivot_table(
                index=["date", "station"],
                columns="datatype",
                values="value",
                aggfunc="first"
            ).reset_index()
            
            return df_pivot
        
        return pd.DataFrame()
    
    def fetch_bulk_data_new_api(self,
                                stations: List[str],
                                start_date: str,
                                end_date: str,
                                datatypes: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Fetch data using newer NCEI Data Service API (more efficient for bulk downloads)
        
        Args:
            stations: List of station IDs
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            datatypes: List of data types (e.g., TMAX, TMIN, PRCP)
        
        Returns:
            DataFrame with weather data
        """
        params = {
            "dataset": "daily-summaries",
            "stations": ",".join(stations),
            "startDate": start_date,
            "endDate": end_date,
            "format": "json",
            "units": "metric"
        }
        
        if datatypes:
            params["dataTypes"] = ",".join(datatypes)
        
        try:
            # Note: This endpoint doesn't use the token header, but still respects rate limits
            response = requests.get(self.data_service_url, params=params, timeout=60)
            response.raise_for_status()
            
            # Parse newline-delimited JSON
            data = [json.loads(line) for line in response.text.strip().split('\n')]
            df = pd.DataFrame(data)
            
            logger.info(f"Fetched {len(df)} records using new API")
            return df
            
        except Exception as e:
            logger.error(f"Error with new API: {e}")
            return pd.DataFrame()
    
    def fetch_date_range_chunked(self,
                                 stationid: str,
                                 start_date: str,
                                 end_date: str,
                                 chunk_days: int = 365) -> pd.DataFrame:
        """
        Fetch large date ranges by chunking into smaller requests
        
        Args:
            stationid: Station ID
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            chunk_days: Number of days per chunk (default: 365)
        
        Returns:
            Combined DataFrame with all data
        """
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        all_chunks = []
        current = start
        
        while current < end:
            chunk_end = min(current + timedelta(days=chunk_days), end)
            
            logger.info(f"Fetching {current.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}")
            
            chunk_df = self.fetch_daily_data(
                stationid,
                current.strftime("%Y-%m-%d"),
                chunk_end.strftime("%Y-%m-%d")
            )
            
            if not chunk_df.empty:
                all_chunks.append(chunk_df)
            
            current = chunk_end + timedelta(days=1)
            
            # Respect rate limits between chunks
            time.sleep(0.5)
        
        if all_chunks:
            combined_df = pd.concat(all_chunks, ignore_index=True)
            return combined_df
        
        return pd.DataFrame()
    
    def save_to_storage(self, 
                       df: pd.DataFrame, 
                       station_id: str,
                       format: str = "parquet",
                       partition_by: str = "year"):
        """
        Save data to local storage with efficient formats
        
        Args:
            df: DataFrame to save
            station_id: Station identifier for file naming
            format: File format (parquet, csv, or feather)
            partition_by: Partition strategy (year, month, or none)
        """
        if df.empty:
            logger.warning("Empty dataframe, nothing to save")
            return
        
        # Ensure date column is datetime
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df['year'] = df['date'].dt.year
            df['month'] = df['date'].dt.month
        
        station_clean = station_id.replace(":", "_")
        station_path = self.storage_path / station_clean
        station_path.mkdir(exist_ok=True)
        
        if partition_by == "none":
            filename = f"{station_clean}_all_data.{format}"
            filepath = station_path / filename
            self._save_file(df, filepath, format)
            
        elif partition_by == "year":
            for year, year_df in df.groupby('year'):
                filename = f"{station_clean}_{year}.{format}"
                filepath = station_path / filename
                self._save_file(year_df, filepath, format)
                
        elif partition_by == "month":
            for (year, month), month_df in df.groupby(['year', 'month']):
                filename = f"{station_clean}_{year}_{month:02d}.{format}"
                filepath = station_path / filename
                self._save_file(month_df, filepath, format)
    
    def _save_file(self, df: pd.DataFrame, filepath: Path, format: str):
        """Save DataFrame in specified format"""
        try:
            if format == "parquet":
                df.to_parquet(filepath, engine='pyarrow', compression='snappy')
            elif format == "feather":
                df.to_feather(filepath)
            elif format == "csv":
                df.to_csv(filepath, index=False)
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            logger.info(f"Saved {len(df)} records to {filepath}")
            
        except Exception as e:
            logger.error(f"Error saving file: {e}")


def main():
    """Example usage"""
    
    # Initialize fetcher with your API token
    API_TOKEN = "YOUR_NOAA_API_TOKEN_HERE"
    STORAGE_PATH = "/path/to/your/massive/drive/weather_data"
    
    fetcher = NOAAWeatherFetcher(API_TOKEN, STORAGE_PATH)
    
    # Example 1: Find stations in North Carolina
    logger.info("Finding stations in North Carolina...")
    stations = fetcher.find_stations(
        locationid="FIPS:37",  # North Carolina
        datatypeid=["TMAX", "TMIN", "PRCP"],
        limit=100
    )
    
    if not stations.empty:
        # Get the first station
        station_id = stations.iloc[0]['id']
        logger.info(f"Using station: {station_id}")
        
        # Example 2: Fetch 10 years of data
        logger.info("Fetching historical data...")
        weather_data = fetcher.fetch_date_range_chunked(
            stationid=station_id,
            start_date="2014-01-01",
            end_date="2024-12-31",
            chunk_days=365
        )
        
        if not weather_data.empty:
            # Save data
            logger.info("Saving data to storage...")
            fetcher.save_to_storage(
                weather_data,
                station_id,
                format="parquet",
                partition_by="year"
            )
    
    # Example 3: Fetch data for multiple stations in parallel
    station_ids = stations['id'].head(5).tolist()
    
    def fetch_station_data(station_id):
        return fetcher.fetch_date_range_chunked(
            station_id,
            "2023-01-01",
            "2024-12-31"
        )
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_station_data, sid): sid for sid in station_ids}
        
        for future in as_completed(futures):
            station_id = futures[future]
            try:
                data = future.result()
                if not data.empty:
                    fetcher.save_to_storage(data, station_id, format="parquet")
            except Exception as e:
                logger.error(f"Error processing {station_id}: {e}")


if __name__ == "__main__":
    main()
