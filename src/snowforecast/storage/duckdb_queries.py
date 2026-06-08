"""
DuckDB Query Examples for Weather Data
=======================================

Examples of querying weather data stored in Parquet format using DuckDB.
DuckDB is a fast, embedded analytical database perfect for querying Parquet files.

Install: pip install duckdb

DuckDB provides SQL queries directly on Parquet files without loading into memory.
"""

import duckdb
from pathlib import Path
from typing import Optional
import pandas as pd


class WeatherQueryEngine:
    """Query engine for weather data using DuckDB"""

    def __init__(self, storage_path: str):
        """
        Initialize query engine

        Args:
            storage_path: Base path where weather data is stored
        """
        self.storage_path = Path(storage_path)
        self.conn = duckdb.connect()  # In-memory database

    def query(self, sql: str) -> pd.DataFrame:
        """Execute SQL query and return DataFrame"""
        return self.conn.execute(sql).df()

    def get_noaa_path(self, station_id: Optional[str] = None) -> str:
        """Get path pattern for NOAA data"""
        base = str(self.storage_path / "noaa")

        if station_id:
            return f"{base}/{station_id}/**/*.parquet"
        else:
            return f"{base}/**/*.parquet"

    def get_openmeteo_path(self, location: Optional[str] = None,
                           data_type: str = "daily") -> str:
        """Get path pattern for Open-Meteo data"""
        base = str(self.storage_path / "openmeteo")

        if location:
            return f"{base}/{location}/{data_type}/**/*.parquet"
        else:
            return f"{base}/**/{data_type}/**/*.parquet"

    # ==========================================================================
    # Example Queries
    # ==========================================================================

    def average_temperature_by_year(self, start_year: int = 2000,
                                   end_year: int = 2024) -> pd.DataFrame:
        """
        Get average temperature by year

        Example output:
            year  avg_temp_max  avg_temp_min
            2020  25.3          15.2
            2021  24.8          14.9
        """
        sql = f"""
        SELECT
            year,
            ROUND(AVG(TMAX) / 10.0, 2) as avg_temp_max_celsius,
            ROUND(AVG(TMIN) / 10.0, 2) as avg_temp_min_celsius,
            ROUND(AVG(PRCP) / 10.0, 2) as avg_precip_mm
        FROM '{self.get_noaa_path()}'
        WHERE year BETWEEN {start_year} AND {end_year}
        GROUP BY year
        ORDER BY year
        """
        return self.query(sql)

    def hottest_days(self, limit: int = 100) -> pd.DataFrame:
        """
        Find the hottest days on record

        Example output:
            date        station       temp_max  location
            2021-07-01  GHCND:XXX     45.2      Death Valley, CA
        """
        sql = f"""
        SELECT
            date,
            station,
            ROUND(TMAX / 10.0, 2) as temp_max_celsius
        FROM '{self.get_noaa_path()}'
        WHERE TMAX IS NOT NULL
        ORDER BY TMAX DESC
        LIMIT {limit}
        """
        return self.query(sql)

    def precipitation_by_month(self, year: int) -> pd.DataFrame:
        """
        Get total precipitation by month for a specific year

        Example output:
            month  total_precip_mm  avg_precip_mm
            1      150.3            4.8
            2      120.1            4.3
        """
        sql = f"""
        SELECT
            month,
            ROUND(SUM(PRCP) / 10.0, 2) as total_precip_mm,
            ROUND(AVG(PRCP) / 10.0, 2) as avg_precip_mm,
            COUNT(*) as days_with_data
        FROM '{self.get_noaa_path()}'
        WHERE year = {year} AND PRCP IS NOT NULL
        GROUP BY month
        ORDER BY month
        """
        return self.query(sql)

    def temperature_extremes(self) -> pd.DataFrame:
        """
        Get temperature extremes across all data

        Example output:
            metric            value  date        station
            Highest Max Temp  56.7   2021-07-01  GHCND:XXX
            Lowest Min Temp   -62.8  1983-12-25  GHCND:YYY
        """
        sql = f"""
        SELECT * FROM (
            SELECT
                'Highest Max Temp' as metric,
                ROUND(MAX(TMAX) / 10.0, 2) as value_celsius,
                date,
                station
            FROM '{self.get_noaa_path()}'
            WHERE TMAX IS NOT NULL
            ORDER BY TMAX DESC
            LIMIT 1
        )
        UNION ALL
        SELECT * FROM (
            SELECT
                'Lowest Min Temp' as metric,
                ROUND(MIN(TMIN) / 10.0, 2) as value_celsius,
                date,
                station
            FROM '{self.get_noaa_path()}'
            WHERE TMIN IS NOT NULL
            ORDER BY TMIN ASC
            LIMIT 1
        )
        """
        return self.query(sql)

    def climate_normals(self, start_year: int = 1991,
                       end_year: int = 2020) -> pd.DataFrame:
        """
        Calculate climate normals (30-year averages)

        Climate normals are 30-year averages used as baselines
        """
        sql = f"""
        SELECT
            month,
            ROUND(AVG(TMAX) / 10.0, 2) as normal_temp_max_celsius,
            ROUND(AVG(TMIN) / 10.0, 2) as normal_temp_min_celsius,
            ROUND(AVG(PRCP) / 10.0, 2) as normal_precip_mm
        FROM '{self.get_noaa_path()}'
        WHERE year BETWEEN {start_year} AND {end_year}
        GROUP BY month
        ORDER BY month
        """
        return self.query(sql)

    def yearly_anomalies(self, baseline_start: int = 1991,
                        baseline_end: int = 2020) -> pd.DataFrame:
        """
        Calculate temperature anomalies compared to baseline period

        Anomalies show how much warmer/cooler each year is vs the baseline
        """
        sql = f"""
        WITH baseline AS (
            SELECT
                AVG(TMAX) / 10.0 as baseline_max,
                AVG(TMIN) / 10.0 as baseline_min
            FROM '{self.get_noaa_path()}'
            WHERE year BETWEEN {baseline_start} AND {baseline_end}
        ),
        yearly_temps AS (
            SELECT
                year,
                AVG(TMAX) / 10.0 as yearly_max,
                AVG(TMIN) / 10.0 as yearly_min
            FROM '{self.get_noaa_path()}'
            GROUP BY year
        )
        SELECT
            y.year,
            ROUND(y.yearly_max, 2) as avg_temp_max,
            ROUND(y.yearly_min, 2) as avg_temp_min,
            ROUND(y.yearly_max - b.baseline_max, 2) as max_temp_anomaly,
            ROUND(y.yearly_min - b.baseline_min, 2) as min_temp_anomaly
        FROM yearly_temps y
        CROSS JOIN baseline b
        ORDER BY y.year
        """
        return self.query(sql)

    def snow_statistics(self, year: int) -> pd.DataFrame:
        """
        Get snow statistics for a specific year

        Example output:
            month  total_snowfall_mm  max_snow_depth_mm  days_with_snow
            12     250.0              150.0              15
        """
        sql = f"""
        SELECT
            month,
            ROUND(SUM(SNOW) / 10.0, 2) as total_snowfall_mm,
            ROUND(MAX(SNWD) / 10.0, 2) as max_snow_depth_mm,
            COUNT(CASE WHEN SNOW > 0 THEN 1 END) as days_with_snow
        FROM '{self.get_noaa_path()}'
        WHERE year = {year}
        GROUP BY month
        HAVING total_snowfall_mm > 0
        ORDER BY month
        """
        return self.query(sql)

    def openmeteo_hourly_summary(self, location: str,
                                 year: int, month: int) -> pd.DataFrame:
        """
        Get hourly weather summary for a location

        Args:
            location: Location name (e.g., "New_York_City")
            year: Year
            month: Month (1-12)
        """
        path = self.get_openmeteo_path(location, "hourly")

        sql = f"""
        SELECT
            time,
            ROUND(temperature_2m, 1) as temp_celsius,
            relative_humidity_2m as humidity_pct,
            ROUND(precipitation, 2) as precip_mm,
            ROUND(wind_speed_10m, 1) as wind_speed_ms
        FROM '{path}'
        WHERE year = {year} AND month = {month}
        ORDER BY time
        """
        return self.query(sql)

    def compare_locations(self, location1: str, location2: str,
                         year: int) -> pd.DataFrame:
        """
        Compare yearly averages between two locations

        Args:
            location1: First location name
            location2: Second location name
            year: Year to compare
        """
        sql = f"""
        WITH loc1 AS (
            SELECT
                '{location1}' as location,
                ROUND(AVG(temperature_2m_mean), 1) as avg_temp,
                ROUND(SUM(precipitation_sum), 1) as total_precip,
                ROUND(AVG(wind_speed_10m_max), 1) as avg_max_wind
            FROM '{self.get_openmeteo_path(location1, "daily")}'
            WHERE year = {year}
        ),
        loc2 AS (
            SELECT
                '{location2}' as location,
                ROUND(AVG(temperature_2m_mean), 1) as avg_temp,
                ROUND(SUM(precipitation_sum), 1) as total_precip,
                ROUND(AVG(wind_speed_10m_max), 1) as avg_max_wind
            FROM '{self.get_openmeteo_path(location2, "daily")}'
            WHERE year = {year}
        )
        SELECT * FROM loc1
        UNION ALL
        SELECT * FROM loc2
        """
        return self.query(sql)

    def data_quality_check(self) -> pd.DataFrame:
        """
        Check data quality and completeness

        Shows missing data percentages by year
        """
        sql = f"""
        SELECT
            year,
            COUNT(*) as total_records,
            ROUND(100.0 * SUM(CASE WHEN TMAX IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_missing_tmax,
            ROUND(100.0 * SUM(CASE WHEN TMIN IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_missing_tmin,
            ROUND(100.0 * SUM(CASE WHEN PRCP IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_missing_prcp
        FROM '{self.get_noaa_path()}'
        GROUP BY year
        ORDER BY year DESC
        LIMIT 20
        """
        return self.query(sql)


# ==============================================================================
# Example Usage
# ==============================================================================

def main():
    """Example usage of the query engine"""

    # Initialize query engine
    engine = WeatherQueryEngine(storage_path="./weather_data")

    print("🔍 Weather Data Query Examples")
    print("=" * 80)
    print()

    # Example 1: Average temperature by year
    print("📊 Average Temperature by Year (2020-2024)")
    print("-" * 80)
    df = engine.average_temperature_by_year(2020, 2024)
    print(df)
    print()

    # Example 2: Hottest days
    print("🔥 Top 10 Hottest Days on Record")
    print("-" * 80)
    df = engine.hottest_days(limit=10)
    print(df)
    print()

    # Example 3: Monthly precipitation
    print("💧 Precipitation by Month (2023)")
    print("-" * 80)
    df = engine.precipitation_by_month(2023)
    print(df)
    print()

    # Example 4: Temperature extremes
    print("🌡️  Temperature Extremes")
    print("-" * 80)
    df = engine.temperature_extremes()
    print(df)
    print()

    # Example 5: Climate normals
    print("📈 Climate Normals (1991-2020)")
    print("-" * 80)
    df = engine.climate_normals()
    print(df)
    print()

    # Example 6: Data quality check
    print("✅ Data Quality Check")
    print("-" * 80)
    df = engine.data_quality_check()
    print(df)
    print()


if __name__ == "__main__":
    main()
