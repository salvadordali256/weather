"""
Unified Weather Data Orchestrator
==================================

Orchestrates weather data collection from multiple sources:
- NOAA NCEI (US focus, station-based, 1763-present)
- Open-Meteo (Global, grid-based, 1940-present)

Features:
- Intelligent source selection
- Parallel processing
- Progress tracking
- Error recovery
- Data validation
- Storage optimization
"""

import pandas as pd
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from tqdm import tqdm
import time

# Import our fetcher classes
from noaa_weather_fetcher import NOAAWeatherFetcher
from openmeteo_weather_fetcher import OpenMeteoWeatherFetcher

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('weather_orchestrator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class WeatherDataOrchestrator:
    """
    Unified orchestrator for weather data collection from multiple sources
    """
    
    def __init__(self, 
                 noaa_api_token: Optional[str],
                 storage_path: str,
                 use_noaa: bool = True,
                 use_openmeteo: bool = True,
                 noaa_user_agent: Optional[str] = None):
        """
        Initialize orchestrator
        
        Args:
            noaa_api_token: NOAA API token (optional while User-Agent is mandatory)
            storage_path: Base path for data storage
            use_noaa: Enable NOAA data source
            use_openmeteo: Enable Open-Meteo data source
            noaa_user_agent: Identifier string required by NOAA (e.g., "(site, email)")
        """
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        # Initialize fetchers
        self.noaa_fetcher = None
        self.openmeteo_fetcher = None
        
        if use_noaa:
            if not noaa_user_agent:
                raise ValueError("noaa_user_agent is required when use_noaa=True.")
            self.noaa_fetcher = NOAAWeatherFetcher(
                noaa_api_token,
                str(self.storage_path / "noaa"),
                user_agent=noaa_user_agent
            )
        
        if use_openmeteo:
            self.openmeteo_fetcher = OpenMeteoWeatherFetcher(
                str(self.storage_path / "openmeteo")
            )
        
        # Metadata tracking
        self.metadata_path = self.storage_path / "metadata"
        self.metadata_path.mkdir(exist_ok=True)
        
        # Progress tracking
        self.progress_file = self.metadata_path / "progress.json"
        self.load_progress()
    
    def load_progress(self):
        """Load progress from previous runs"""
        if self.progress_file.exists():
            with open(self.progress_file, 'r') as f:
                self.progress = json.load(f)
        else:
            self.progress = {
                "completed_locations": [],
                "failed_locations": [],
                "last_run": None
            }
    
    def save_progress(self):
        """Save progress to file"""
        self.progress["last_run"] = datetime.now().isoformat()
        with open(self.progress_file, 'w') as f:
            json.dump(self.progress, f, indent=2)
    
    def fetch_us_comprehensive(self,
                              states: Optional[List[str]] = None,
                              start_date: str = "1950-01-01",
                              end_date: Optional[str] = None,
                              max_stations_per_state: int = 50) -> Dict:
        """
        Fetch comprehensive US weather data using NOAA
        
        Args:
            states: List of state FIPS codes (None for all states)
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format (None for current date)
            max_stations_per_state: Maximum stations to fetch per state
        
        Returns:
            Dictionary with summary statistics
        """
        if not self.noaa_fetcher:
            raise ValueError("NOAA fetcher not initialized")
        
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        
        # US state FIPS codes
        if states is None:
            states = [
                "01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
                "13", "15", "16", "17", "18", "19", "20", "21", "22", "23",
                "24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
                "34", "35", "36", "37", "38", "39", "40", "41", "42", "44",
                "45", "46", "47", "48", "49", "50", "51", "53", "54", "55", "56"
            ]
        
        state_names = {
            "01": "Alabama", "02": "Alaska", "04": "Arizona", "05": "Arkansas",
            "06": "California", "08": "Colorado", "09": "Connecticut", "10": "Delaware",
            "11": "District_of_Columbia", "12": "Florida", "13": "Georgia", "15": "Hawaii",
            "16": "Idaho", "17": "Illinois", "18": "Indiana", "19": "Iowa",
            "20": "Kansas", "21": "Kentucky", "22": "Louisiana", "23": "Maine",
            "24": "Maryland", "25": "Massachusetts", "26": "Michigan", "27": "Minnesota",
            "28": "Mississippi", "29": "Missouri", "30": "Montana", "31": "Nebraska",
            "32": "Nevada", "33": "New_Hampshire", "34": "New_Jersey", "35": "New_Mexico",
            "36": "New_York", "37": "North_Carolina", "38": "North_Dakota", "39": "Ohio",
            "40": "Oklahoma", "41": "Oregon", "42": "Pennsylvania", "44": "Rhode_Island",
            "45": "South_Carolina", "46": "South_Dakota", "47": "Tennessee", "48": "Texas",
            "49": "Utah", "50": "Vermont", "51": "Virginia", "53": "Washington",
            "54": "West_Virginia", "55": "Wisconsin", "56": "Wyoming"
        }
        
        summary = {
            "total_stations": 0,
            "successful_downloads": 0,
            "failed_downloads": 0,
            "states_processed": []
        }
        
        for state_fips in tqdm(states, desc="Processing states"):
            state_name = state_names.get(state_fips, f"State_{state_fips}")
            
            logger.info(f"Processing {state_name}...")
            
            # Find stations in this state
            stations = self.noaa_fetcher.find_stations(
                locationid=f"FIPS:{state_fips}",
                datatypeid=["TMAX", "TMIN", "PRCP"],
                limit=max_stations_per_state
            )
            
            if stations.empty:
                logger.warning(f"No stations found for {state_name}")
                continue
            
            summary["total_stations"] += len(stations)
            
            # Process each station
            for idx, station in stations.iterrows():
                station_id = station['id']
                
                # Check if already processed
                if station_id in self.progress["completed_locations"]:
                    logger.info(f"Skipping {station_id} (already processed)")
                    continue
                
                try:
                    # Fetch data
                    data = self.noaa_fetcher.fetch_date_range_chunked(
                        station_id,
                        start_date,
                        end_date,
                        chunk_days=365
                    )
                    
                    if not data.empty:
                        # Save data
                        self.noaa_fetcher.save_to_storage(
                            data,
                            station_id,
                            format="parquet",
                            partition_by="year"
                        )
                        
                        summary["successful_downloads"] += 1
                        self.progress["completed_locations"].append(station_id)
                        self.save_progress()
                        
                        logger.info(f"✓ {station_id}: {len(data)} records")
                    
                except Exception as e:
                    logger.error(f"✗ {station_id}: {e}")
                    summary["failed_downloads"] += 1
                    self.progress["failed_locations"].append({
                        "station_id": station_id,
                        "error": str(e),
                        "timestamp": datetime.now().isoformat()
                    })
                    self.save_progress()
                
                # Rate limiting
                time.sleep(0.2)
            
            summary["states_processed"].append(state_name)
        
        # Save summary
        summary_file = self.metadata_path / f"us_download_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Download complete. Summary saved to {summary_file}")
        return summary
    
    def fetch_global_grid(self,
                         region_bounds: Tuple[float, float, float, float],
                         resolution: float = 0.5,
                         start_date: str = "1940-01-01",
                         end_date: Optional[str] = None,
                         max_workers: int = 5) -> Dict:
        """
        Fetch global weather data on a grid using Open-Meteo
        
        Args:
            region_bounds: (north, south, east, west) in degrees
            resolution: Grid resolution in degrees
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format (None for current date)
            max_workers: Maximum parallel workers
        
        Returns:
            Dictionary with summary statistics
        """
        if not self.openmeteo_fetcher:
            raise ValueError("Open-Meteo fetcher not initialized")
        
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        
        north, south, east, west = region_bounds
        
        # Create grid points
        grid_points = self.openmeteo_fetcher.create_grid_points(
            north, south, east, west, resolution
        )
        
        logger.info(f"Created grid with {len(grid_points)} points")
        
        summary = {
            "total_points": len(grid_points),
            "successful_downloads": 0,
            "failed_downloads": 0
        }
        
        def fetch_grid_point(point):
            lat, lon = point
            location_name = f"Grid_{lat:.2f}_{lon:.2f}"
            
            # Check if already processed
            if location_name in self.progress["completed_locations"]:
                return {"status": "skipped", "location": location_name}
            
            try:
                data = self.openmeteo_fetcher.fetch_date_range_chunked(
                    lat, lon, start_date, end_date, chunk_years=10
                )
                
                if data:
                    self.openmeteo_fetcher.save_to_storage(
                        data, location_name, format="parquet", partition_by="year"
                    )
                    
                    self.progress["completed_locations"].append(location_name)
                    return {"status": "success", "location": location_name}
                
            except Exception as e:
                logger.error(f"Error fetching {location_name}: {e}")
                return {"status": "failed", "location": location_name, "error": str(e)}
        
        # Process grid points in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(fetch_grid_point, point) for point in grid_points]
            
            for future in tqdm(as_completed(futures), total=len(futures), desc="Grid points"):
                result = future.result()
                
                if result["status"] == "success":
                    summary["successful_downloads"] += 1
                elif result["status"] == "failed":
                    summary["failed_downloads"] += 1
                    self.progress["failed_locations"].append(result)
        
        self.save_progress()
        
        # Save summary
        summary_file = self.metadata_path / f"grid_download_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Grid download complete. Summary saved to {summary_file}")
        return summary
    
    def fetch_major_cities(self,
                          start_date: str = "1940-01-01",
                          end_date: Optional[str] = None) -> Dict:
        """
        Fetch weather data for major world cities using Open-Meteo
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format (None for current date)
        
        Returns:
            Dictionary with summary statistics
        """
        if not self.openmeteo_fetcher:
            raise ValueError("Open-Meteo fetcher not initialized")
        
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        
        # Major world cities
        cities = [
            # North America
            ("New_York_USA", 40.7128, -74.0060),
            ("Los_Angeles_USA", 34.0522, -118.2437),
            ("Chicago_USA", 41.8781, -87.6298),
            ("Toronto_Canada", 43.6532, -79.3832),
            ("Mexico_City_Mexico", 19.4326, -99.1332),
            
            # Europe
            ("London_UK", 51.5074, -0.1278),
            ("Paris_France", 48.8566, 2.3522),
            ("Berlin_Germany", 52.5200, 13.4050),
            ("Moscow_Russia", 55.7558, 37.6173),
            ("Rome_Italy", 41.9028, 12.4964),
            
            # Asia
            ("Tokyo_Japan", 35.6762, 139.6503),
            ("Beijing_China", 39.9042, 116.4074),
            ("Shanghai_China", 31.2304, 121.4737),
            ("Mumbai_India", 19.0760, 72.8777),
            ("Seoul_South_Korea", 37.5665, 126.9780),
            
            # South America
            ("São_Paulo_Brazil", -23.5505, -46.6333),
            ("Buenos_Aires_Argentina", -34.6037, -58.3816),
            ("Lima_Peru", -12.0464, -77.0428),
            
            # Africa
            ("Cairo_Egypt", 30.0444, 31.2357),
            ("Lagos_Nigeria", 6.5244, 3.3792),
            ("Johannesburg_South_Africa", -26.2041, 28.0473),
            
            # Oceania
            ("Sydney_Australia", -33.8688, 151.2093),
            ("Melbourne_Australia", -37.8136, 144.9631),
        ]
        
        logger.info(f"Fetching data for {len(cities)} major cities...")
        
        all_data = self.openmeteo_fetcher.fetch_multiple_locations(
            cities,
            start_date,
            end_date,
            max_workers=5
        )
        
        summary = {
            "total_cities": len(cities),
            "successful_downloads": 0,
            "failed_downloads": 0
        }
        
        for city_name, city_data in all_data.items():
            if city_data:
                self.openmeteo_fetcher.save_to_storage(
                    city_data,
                    city_name,
                    format="parquet",
                    partition_by="year"
                )
                summary["successful_downloads"] += 1
                logger.info(f"✓ {city_name}")
            else:
                summary["failed_downloads"] += 1
                logger.error(f"✗ {city_name}")
        
        # Save summary
        summary_file = self.metadata_path / f"cities_download_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"City download complete. Summary saved to {summary_file}")
        return summary
    
    def generate_report(self) -> str:
        """Generate a comprehensive report of all collected data"""
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("WEATHER DATA COLLECTION REPORT")
        report_lines.append("=" * 80)
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"Storage Path: {self.storage_path}")
        report_lines.append("")
        
        # Progress summary
        report_lines.append("PROGRESS SUMMARY")
        report_lines.append("-" * 80)
        report_lines.append(f"Completed Locations: {len(self.progress['completed_locations'])}")
        report_lines.append(f"Failed Locations: {len(self.progress['failed_locations'])}")
        report_lines.append(f"Last Run: {self.progress.get('last_run', 'Never')}")
        report_lines.append("")
        
        # Storage summary
        report_lines.append("STORAGE SUMMARY")
        report_lines.append("-" * 80)
        
        total_size = 0
        file_count = 0
        
        for path in [self.storage_path / "noaa", self.storage_path / "openmeteo"]:
            if path.exists():
                for file in path.rglob("*.parquet"):
                    total_size += file.stat().st_size
                    file_count += 1
        
        report_lines.append(f"Total Files: {file_count:,}")
        report_lines.append(f"Total Size: {total_size / (1024**3):.2f} GB")
        report_lines.append("")
        
        report_text = "\n".join(report_lines)
        
        # Save report
        report_file = self.metadata_path / f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(report_file, 'w') as f:
            f.write(report_text)
        
        print(report_text)
        return report_text


def main():
    """Example orchestration workflows"""
    
    # Configuration
    NOAA_API_TOKEN = "YOUR_NOAA_TOKEN_HERE"
    NOAA_USER_AGENT = "(salvadordali256.net, kyle@salvadordali256.net)"
    STORAGE_PATH = "/mnt/massive_drive/weather_data"
    
    # Initialize orchestrator
    orchestrator = WeatherDataOrchestrator(
        noaa_api_token=NOAA_API_TOKEN,
        storage_path=STORAGE_PATH,
        use_noaa=True,
        use_openmeteo=True,
        noaa_user_agent=NOAA_USER_AGENT
    )
    
    # Example 1: Fetch comprehensive US data
    logger.info("Starting US comprehensive download...")
    us_summary = orchestrator.fetch_us_comprehensive(
        states=["06", "36", "48"],  # California, New York, Texas
        start_date="2000-01-01",
        max_stations_per_state=20
    )
    
    # Example 2: Fetch global grid for North America
    logger.info("Starting North America grid download...")
    na_summary = orchestrator.fetch_global_grid(
        region_bounds=(70, 25, -50, -170),  # North America
        resolution=1.0,  # ~111km spacing
        start_date="1980-01-01",
        max_workers=5
    )
    
    # Example 3: Fetch major world cities
    logger.info("Starting major cities download...")
    cities_summary = orchestrator.fetch_major_cities(
        start_date="1950-01-01"
    )
    
    # Generate comprehensive report
    orchestrator.generate_report()


if __name__ == "__main__":
    main()
